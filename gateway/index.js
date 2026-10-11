/**
 * MultibaggerX WhatsApp gateway (free tier friendly).
 *
 *   CRM (Vercel) ──writes──▶ crm_wa_messages (Supabase Postgres) ◀──claims one at a time── this gateway ──▶ WhatsApp
 *
 * - Baileys (WhatsApp Web protocol over a websocket, no Chromium): ~100 MB RAM, fits Render's free 512 MB.
 * - The WhatsApp login lives in Postgres (crm_wa_auth), so restarts, deploys and Render's free-tier sleep never
 *   need a new QR scan.
 * - Linking is done from the CRM (Settings -> WhatsApp -> Connection): it writes a command into crm_wa_state and
 *   shows the QR / pairing code this process writes back.
 * - Sends one message at a time with a randomised gap, a daily cap and quiet hours (all from crm_settings
 *   "whatsapp"), checks the number is on WhatsApp first, and logs every send on the lead's CRM timeline.
 * - Replies from leads are logged on their timeline; "STOP" opts them out and cancels anything queued.
 * - A Postgres advisory lock makes sure only one copy runs (two copies would knock each other's session off).
 *
 * Env: DATABASE_URL (Supabase *session* pooler, port 5432), PORT (Render sets it).
 * HTTP: GET /health only (Supabase pg_cron pings it every 10 minutes so the free instance never sleeps).
 */
const http = require("node:http");
const { Pool } = require("pg");
const pino = require("pino");
const QRCode = require("qrcode");
const {
  default: makeWASocket,
  BufferJSON,
  Browsers,
  DisconnectReason,
  fetchLatestBaileysVersion,
  initAuthCreds,
  makeCacheableSignalKeyStore,
  proto,
} = require("baileys");

const LOCK_ID = 727272001;
const TZ_OFFSET_MIN = 330; // IST
const logger = pino({ level: process.env.LOG_LEVEL || "warn" });
const log = (...a) => console.log(new Date().toISOString(), ...a);

if (!process.env.DATABASE_URL) {
  console.error("DATABASE_URL is not set");
  process.exit(1);
}
const pool = new Pool({
  connectionString: process.env.DATABASE_URL.replace(/[?&]sslmode=[^&]*/g, ""),
  ssl: /localhost|127\.0\.0\.1/.test(process.env.DATABASE_URL) ? undefined : { rejectUnauthorized: false },
  max: 4,
  idleTimeoutMillis: 30_000,
});
pool.on("error", (e) => log("[pg] pool error", e.message));

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

/* ---------------- shared state row ---------------- */

async function setState(patch) {
  const keys = Object.keys(patch);
  if (!keys.length) return;
  const cols = keys.map((k, i) => `"${k}" = $${i + 1}`).join(", ");
  await pool.query(`update crm_wa_state set ${cols}, heartbeat_at = now() where id = 1`, keys.map((k) => patch[k])).catch((e) => log("[state]", e.message));
}

async function settings() {
  const { rows } = await pool.query("select value from crm_settings where key = 'whatsapp'");
  const v = rows[0]?.value ?? {};
  return {
    gapSeconds: Math.max(4, Number(v.gapSeconds) || 10),
    dailyCap: Math.max(1, Number(v.dailyCap) || 150),
    quietStart: Number.isFinite(Number(v.quietStart)) ? Number(v.quietStart) : 21,
    quietEnd: Number.isFinite(Number(v.quietEnd)) ? Number(v.quietEnd) : 8,
  };
}

/* ---------------- WhatsApp login stored in Postgres ---------------- */

async function usePgAuthState() {
  const read = async (key) => {
    const { rows } = await pool.query("select value from crm_wa_auth where key = $1", [key]);
    return rows[0] ? JSON.parse(rows[0].value, BufferJSON.reviver) : null;
  };
  const write = (key, data) =>
    pool.query("insert into crm_wa_auth (key, value) values ($1, $2) on conflict (key) do update set value = excluded.value", [key, JSON.stringify(data, BufferJSON.replacer)]);
  const creds = (await read("creds")) || initAuthCreds();
  return {
    state: {
      creds,
      keys: {
        get: async (type, ids) => {
          const out = {};
          if (!ids.length) return out;
          const { rows } = await pool.query("select key, value from crm_wa_auth where key = any($1)", [ids.map((id) => `${type}-${id}`)]);
          for (const r of rows) {
            let v = JSON.parse(r.value, BufferJSON.reviver);
            if (type === "app-state-sync-key" && v) v = proto.Message.AppStateSyncKeyData.fromObject(v);
            out[r.key.slice(type.length + 1)] = v;
          }
          return out;
        },
        set: async (data) => {
          const client = await pool.connect();
          try {
            await client.query("begin");
            for (const type of Object.keys(data)) {
              for (const id of Object.keys(data[type])) {
                const key = `${type}-${id}`;
                const value = data[type][id];
                if (value) await client.query("insert into crm_wa_auth (key, value) values ($1, $2) on conflict (key) do update set value = excluded.value", [key, JSON.stringify(value, BufferJSON.replacer)]);
                else await client.query("delete from crm_wa_auth where key = $1", [key]);
              }
            }
            await client.query("commit");
          } catch (e) {
            await client.query("rollback").catch(() => {});
            throw e;
          } finally {
            client.release();
          }
        },
      },
    },
    saveCreds: () => write("creds", creds),
  };
}

const clearAuth = () => pool.query("delete from crm_wa_auth");

/* ---------------- connection ---------------- */

let sock = null;
let connected = false;
let starting = false;
let linkUntil = 0; // when not linked, only show QRs while someone is actually trying to link (CRM button)
let pairPhone = null;
let pairRequested = false;
let retry = 0;

async function isRegistered() {
  const { rows } = await pool.query("select value from crm_wa_auth where key = 'creds'");
  if (!rows[0]) return false;
  try {
    return Boolean(JSON.parse(rows[0].value, BufferJSON.reviver).registered);
  } catch {
    return false;
  }
}

async function start() {
  if (starting || sock) return;
  starting = true;
  try {
    const registered = await isRegistered();
    if (!registered && Date.now() > linkUntil) {
      await setState({ status: "not_linked", qr_svg: null, pairing_code: null, me: null });
      return;
    }
    const { state, saveCreds } = await usePgAuthState();
    let version;
    try {
      version = (await fetchLatestBaileysVersion()).version;
    } catch {}
    await setState({ status: "connecting", error: null });
    log("[conn] connecting", registered ? "(linked)" : "(linking)", version ? version.join(".") : "");
    const s = makeWASocket({
      version,
      auth: { creds: state.creds, keys: makeCacheableSignalKeyStore(state.keys, logger) },
      logger,
      browser: Browsers.ubuntu("MultibaggerX CRM"),
      markOnlineOnConnect: false,
      syncFullHistory: false,
      shouldSyncHistoryMessage: () => false,
      generateHighQualityLinkPreview: false,
    });
    sock = s;
    pairRequested = false;
    s.ev.on("creds.update", saveCreds);
    s.ev.on("connection.update", (u) => onConnection(s, u).catch((e) => log("[conn]", e.message)));
    s.ev.on("messages.upsert", (m) => onIncoming(m).catch((e) => log("[in]", e.message)));
  } finally {
    starting = false;
  }
}

async function onConnection(s, u) {
  if (s !== sock) return;
  if (u.qr) {
    if (pairPhone && !pairRequested) {
      pairRequested = true;
      try {
        const code = await s.requestPairingCode(pairPhone);
        await setState({ status: "qr", pairing_code: code, qr_svg: await QRCode.toString(u.qr, { type: "svg", margin: 1 }) });
        log("[link] pairing code issued for", pairPhone.slice(0, 4) + "******");
        return;
      } catch (e) {
        await setState({ error: `Pairing code failed: ${e.message}` });
      }
    }
    await setState({ status: "qr", qr_svg: await QRCode.toString(u.qr, { type: "svg", margin: 1 }) });
  }
  if (u.connection === "open") {
    connected = true;
    retry = 0;
    linkUntil = 0;
    pairPhone = null;
    const me = String(s.user?.id ?? "").split(":")[0].split("@")[0];
    await setState({ status: "connected", me, qr_svg: null, pairing_code: null, error: null, connected_at: new Date() });
    log("[conn] connected as", me.slice(0, 4) + "******");
  }
  if (u.connection === "close") {
    connected = false;
    sock = null;
    const code = u.lastDisconnect?.error?.output?.statusCode;
    log("[conn] closed", code, u.lastDisconnect?.error?.message ?? "");
    if (code === DisconnectReason.loggedOut) {
      await clearAuth();
      await setState({ status: "logged_out", me: null, qr_svg: null, pairing_code: null, error: "WhatsApp was unlinked from the phone. Link it again." });
      return;
    }
    if (code === DisconnectReason.connectionReplaced) {
      await setState({ status: "offline", error: "Another copy of the gateway took over this WhatsApp session." });
      await sleep(60_000);
    }
    retry = Math.min(retry + 1, 8);
    setTimeout(() => start().catch((e) => log("[start]", e.message)), Math.min(60_000, 2000 * 2 ** retry));
  }
}

/* ---------------- incoming replies -> CRM timeline ---------------- */

const STOP = /^\s*(stop|unsubscribe|band karo|bandh karo)\s*[.!]*\s*$/i;

function textOf(msg) {
  const m = msg.message || {};
  return (
    m.conversation ||
    m.extendedTextMessage?.text ||
    m.imageMessage?.caption ||
    m.videoMessage?.caption ||
    m.buttonsResponseMessage?.selectedDisplayText ||
    m.listResponseMessage?.title ||
    (m.imageMessage ? "[photo]" : m.audioMessage ? "[voice note]" : m.documentMessage ? "[document]" : m.videoMessage ? "[video]" : m.stickerMessage ? "[sticker]" : "")
  );
}

async function onIncoming({ messages, type }) {
  if (type !== "notify") return;
  for (const msg of messages) {
    const jid = msg.key?.remoteJid || "";
    if (msg.key?.fromMe || !jid || jid.endsWith("@g.us") || jid === "status@broadcast" || jid.endsWith("@newsletter")) continue;
    const pnJid = jid.endsWith("@s.whatsapp.net") ? jid : msg.key?.senderPn || msg.key?.participantPn || "";
    const digits = String(pnJid).split("@")[0].split(":")[0];
    if (!/^\d{8,15}$/.test(digits)) continue;
    const text = textOf(msg);
    if (!text) continue;
    const { rows } = await pool.query("select id, owner_id, name, tags from crm_leads where phone = $1 and deleted_at is null order by id limit 1", [`+${digits}`]);
    const lead = rows[0];
    if (!lead) continue;
    await pool.query(
      "insert into crm_activities (lead_id, user_id, type, body, meta) values ($1, null, 'whatsapp', $2, $3)",
      [lead.id, text.slice(0, 4000), JSON.stringify({ direction: "in", via: "gateway", waId: msg.key.id })],
    );
    await pool.query("update crm_leads set updated_at = now() where id = $1", [lead.id]);
    if (STOP.test(text)) {
      const tags = Array.from(new Set([...(lead.tags || []), "whatsapp-stop"]));
      await pool.query("update crm_leads set whatsapp_opt_in = false, tags = $2 where id = $1", [lead.id, tags]);
      await pool.query("update crm_wa_messages set status = 'cancelled', error = 'Lead replied STOP' where lead_id = $1 and status = 'queued'", [lead.id]);
    }
    if (lead.owner_id) {
      await pool.query("insert into crm_notifications (user_id, body, lead_id) values ($1, $2, $3)", [lead.owner_id, `${lead.name} replied on WhatsApp: ${text.slice(0, 120)}`, lead.id]).catch(() => {});
    }
  }
}

/* ---------------- outbox worker ---------------- */

function istHour(d = new Date()) {
  return new Date(d.getTime() + TZ_OFFSET_MIN * 60_000).getUTCHours();
}
function inQuietHours(cfg) {
  const h = istHour();
  if (cfg.quietStart === cfg.quietEnd) return false;
  return cfg.quietStart > cfg.quietEnd ? h >= cfg.quietStart || h < cfg.quietEnd : h >= cfg.quietStart && h < cfg.quietEnd;
}

async function claim() {
  const { rows } = await pool.query(`
    update crm_wa_messages set status = 'sending', attempts = attempts + 1
    where id = (
      select id from crm_wa_messages
      where (status = 'queued' and not_before <= now())
         or (status = 'sending' and created_at < now() - interval '10 minutes' and attempts < 3)
      order by not_before, id
      limit 1
      for update skip locked
    )
    returning id, lead_id, phone, body, source, step_id, campaign_id, attempts, created_by_id`);
  return rows[0] || null;
}

async function sentToday() {
  const { rows } = await pool.query(
    "select count(*)::int as n from crm_wa_messages where status = 'sent' and sent_at >= date_trunc('day', now() at time zone 'Asia/Kolkata') at time zone 'Asia/Kolkata'",
  );
  return rows[0].n;
}

async function sendOne(m) {
  const digits = String(m.phone).replace(/\D/g, "");
  const number = digits.length === 10 ? `91${digits}` : digits;
  const [hit] = await sock.onWhatsApp(`${number}@s.whatsapp.net`);
  if (!hit?.exists) throw Object.assign(new Error("This number is not on WhatsApp."), { permanent: true });
  const jid = hit.jid;
  // Look like a person: show "typing…" for a moment proportional to the message length.
  await sock.presenceSubscribe(jid).catch(() => {});
  await sock.sendPresenceUpdate("composing", jid).catch(() => {});
  await sleep(Math.min(4000, 1200 + m.body.length * 15));
  await sock.sendPresenceUpdate("paused", jid).catch(() => {});
  const res = await sock.sendMessage(jid, { text: m.body });
  return res?.key?.id ?? null;
}

async function work() {
  for (;;) {
    try {
      if (!connected || !sock) {
        await sleep(5000);
        continue;
      }
      const cfg = await settings();
      if (inQuietHours(cfg)) {
        await sleep(60_000);
        continue;
      }
      if ((await sentToday()) >= cfg.dailyCap) {
        await sleep(5 * 60_000);
        continue;
      }
      const m = await claim();
      if (!m) {
        await sleep(4000);
        continue;
      }
      try {
        const waId = await sendOne(m);
        await pool.query("update crm_wa_messages set status = 'sent', sent_at = now(), wa_id = $2, error = null where id = $1", [m.id, waId]);
        if (m.lead_id && m.source !== "test") {
          await pool.query("insert into crm_activities (lead_id, user_id, type, body, meta) values ($1, $2, 'whatsapp', $3, $4)", [
            m.lead_id,
            m.created_by_id,
            m.body,
            JSON.stringify({ direction: "out", via: "gateway", automated: m.source !== "manual", source: m.source, stepId: m.step_id, campaignId: m.campaign_id, waId }),
          ]);
          await pool.query("update crm_leads set updated_at = now() where id = $1", [m.lead_id]);
        }
        log("[send] sent", m.id, m.source);
      } catch (e) {
        const permanent = e.permanent || m.attempts >= 3;
        await pool.query(
          permanent
            ? "update crm_wa_messages set status = 'failed', error = $2 where id = $1"
            : "update crm_wa_messages set status = 'queued', error = $2, not_before = now() + interval '2 minutes' where id = $1",
          [m.id, String(e.message || e).slice(0, 500)],
        );
        log("[send] failed", m.id, e.message);
      }
      // Human-ish pace between messages: gap .. 1.6 x gap seconds.
      await sleep(cfg.gapSeconds * 1000 * (1 + Math.random() * 0.6));
    } catch (e) {
      log("[worker]", e.message);
      await sleep(10_000);
    }
  }
}

/* ---------------- commands from the CRM ---------------- */

async function commands() {
  for (;;) {
    try {
      // RETURNING gives the updated row, so read the command through a CTE before clearing it.
      const { rows } = await pool.query(`
        with c as (select command, command_arg from crm_wa_state where id = 1 and command is not null for update)
        update crm_wa_state s set command = null, command_arg = null from c where s.id = 1
        returning c.command, c.command_arg`);
      const c = rows[0];
      if (c) log("[cmd]", c.command);
      if (c?.command === "link" || c?.command === "pair") {
        linkUntil = Date.now() + 5 * 60_000;
        pairPhone = c.command === "pair" ? String(c.command_arg || "").replace(/\D/g, "") || null : null;
        if (pairPhone && pairPhone.length === 10) pairPhone = `91${pairPhone}`;
        if (sock && !connected) {
          sock.end(undefined);
          sock = null;
        }
        await setState({ pairing_code: null, qr_svg: null, error: null });
        await start();
      } else if (c?.command === "logout") {
        if (sock) await sock.logout().catch(() => {});
        sock = null;
        connected = false;
        await clearAuth();
        await setState({ status: "not_linked", me: null, qr_svg: null, pairing_code: null, error: null });
      }
      // Linking window over and still not linked: stop generating QRs.
      if (!connected && sock && linkUntil && Date.now() > linkUntil && !(await isRegistered())) {
        sock.end(undefined);
        sock = null;
        linkUntil = 0;
        await setState({ status: "not_linked", qr_svg: null, pairing_code: null });
      }
      if (!sock && !starting && (await isRegistered())) await start();
    } catch (e) {
      log("[cmd]", e.message);
    }
    await sleep(3000);
  }
}

/* ---------------- keep-awake ---------------- */

/**
 * Render's free plan sleeps after 15 minutes without visits. Supabase pg_cron pings /health every 10 minutes;
 * Render tells each service its public address (RENDER_EXTERNAL_URL), so the gateway points that job at itself
 * on every boot. The address is also saved in crm_settings ("wa_gateway") for reference.
 */
async function registerKeepalive() {
  const url = (process.env.RENDER_EXTERNAL_URL || "").replace(/\/$/, "");
  if (!/^https:\/\/[a-z0-9.-]+$/i.test(url)) return;
  try {
    await pool.query("select cron.schedule('wa-gateway-keepalive', '*/10 * * * *', $1)", [`select net.http_get('${url}/health')`]);
    await pool.query(
      "insert into crm_settings (key, value, updated_at) values ('wa_gateway', $1, now()) on conflict (key) do update set value = excluded.value, updated_at = now()",
      [JSON.stringify({ url })],
    );
    log("[keepalive] pinging", url);
  } catch (e) {
    log("[keepalive] could not register:", e.message);
  }
}

/* ---------------- boot ---------------- */

let lockClient = null;

async function acquireLock() {
  for (;;) {
    try {
      lockClient = await pool.connect();
      const { rows } = await lockClient.query("select pg_try_advisory_lock($1) as ok", [LOCK_ID]);
      if (rows[0].ok) return;
      lockClient.release();
      lockClient = null;
      log("[lock] another gateway is running; standing by");
    } catch (e) {
      log("[lock]", e.message);
      lockClient?.release();
      lockClient = null;
    }
    await sleep(15_000);
  }
}

const server = http.createServer((req, res) => {
  if (req.url === "/health" || req.url === "/") {
    res.writeHead(200, { "content-type": "application/json" });
    res.end(JSON.stringify({ ok: true, active: Boolean(lockClient), connected }));
    return;
  }
  res.writeHead(404).end();
});
server.listen(Number(process.env.PORT) || 3000, () => log("[http] listening on", Number(process.env.PORT) || 3000));

(async () => {
  await acquireLock();
  log("[lock] acquired; this copy is active");
  setInterval(() => pool.query("update crm_wa_state set heartbeat_at = now() where id = 1").catch(() => {}), 30_000);
  await pool.query("update crm_wa_state set heartbeat_at = now() where id = 1");
  await registerKeepalive();
  start().catch((e) => log("[start]", e.message));
  commands();
  work();
})();

async function shutdown() {
  log("[exit] shutting down");
  try {
    sock?.end(undefined);
    await setState({ status: "offline" });
    await lockClient?.query("select pg_advisory_unlock($1)", [LOCK_ID]);
    lockClient?.release();
    await pool.end();
  } catch {}
  process.exit(0);
}
process.on("SIGTERM", shutdown);
process.on("SIGINT", shutdown);

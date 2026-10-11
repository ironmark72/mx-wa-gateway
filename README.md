# MultibaggerX WhatsApp gateway (free)

Sends the CRM's WhatsApp messages from your own WhatsApp number. Costs nothing: it runs on Render's free plan
and keeps everything in the CRM's existing Supabase database.

```
Website form ─▶ CRM (Vercel) ─▶ crm_wa_messages (Supabase) ◀─ gateway (Render, free) ─▶ WhatsApp
                                                    ▲                      │
                              CRM shows status, QR  └── crm_wa_state ◀─────┘ (status, QR, replies)
```

- **No MSG91, no n8n, no Chromium.** The gateway uses Baileys (WhatsApp Web over a websocket, ~100 MB of memory).
- **Messages are edited in the CRM:** WhatsApp, then Message flow. The thank-you plus group link on booking, the reply to contact
  forms, reminders before the workshop, and a confirmation when a lead is marked Won. Broadcasts and the "Send WhatsApp"
  workflow action use the same queue.
- **The WhatsApp login lives in the database** (`crm_wa_auth`), so restarts and deploys never need a new scan.
- **Linking is done from the CRM:** WhatsApp, then Connection. Use "Show QR code", or "Get code" when you only have the phone.
- **Safe pace:** one message every 10 to 16 seconds, at most 150 a day, nothing between 9 PM and 8 AM (all editable under Connection).
  A "typing…" indicator shows before each message. Numbers not on WhatsApp are skipped.
- **Replies** appear on the lead's timeline and notify the owner. A reply of **STOP** opts the lead out for good.

## Deploy (one time, about 5 minutes)

1. Make a free account at **render.com** (sign in with GitHub, account `ironmark72`). No card is needed for the free plan.
2. Render: **New, then Blueprint**. Pick the repo **ironmark72/wa-gateway** and click Apply.
3. When it asks for **DATABASE_URL**, paste the single line from `D:\WA\gateway\RENDER-DATABASE-URL.txt`.
   That file is on this PC only and never goes to GitHub.
4. Wait for "Live" (about 3 minutes). Then in the CRM open **WhatsApp, then Connection**. It should say "Not linked",
   which means the gateway is running.
5. Click **Show QR code** and scan it from the business phone: WhatsApp, Settings, Linked devices, Link a device.

### Keeping it awake (already set up)

Render's free plan sleeps after 15 minutes without visits. Supabase pings `https://mx-wa-gateway.onrender.com/health`
every 10 minutes (`pg_cron` job `wa-gateway-keepalive`), so it never sleeps. If Render gives the service a different
address, change it in Supabase:

```sql
select cron.alter_job((select jobid from cron.job where jobname = 'wa-gateway-keepalive'),
  command := $$select net.http_get('https://YOUR-NAME.onrender.com/health')$$);
```

Even while it sleeps, nothing is lost: messages wait in the queue and go out when it wakes.

## Run it on this PC instead (optional)

```powershell
cd D:\WA\gateway
npm install
$env:DATABASE_URL = Get-Content RENDER-DATABASE-URL.txt
node index.js
```

Only one copy sends at a time (a database lock), so the PC and Render can't knock each other off.

## Good to know

- This is an unofficial WhatsApp Web connection, like WhatsApp Desktop. Keep the pace settings as they are, only message
  people who gave their number, and don't send bulk promotions. Broadcasts go to your own leads only, one by one.
- The free plan has 750 hours a month, enough for one service running all the time.
- The earlier n8n and wwebjs setup (paid hosting, about $25 a month) is no longer used and is not part of this repo.

# AI Reception Builder

Φτιάχνεις AI τηλεφωνική ρεσεψιόν με το παρακάτω stack (~$0.018/λεπτό):

## Stack
- **Transport**: Telnyx (SIP) + FastAPIWebsocketTransport + TelnyxFrameSerializer
- **STT**: Deepgram Nova-2 (ελληνικά `language="el"`)
- **LLM**: GPT-4o-mini
- **TTS**: OpenAI TTS-1, φωνή `nova`
- **Framework**: pipecat-ai + FastAPI + uvicorn

## Αρχεία που φτιάχνεις

### ss.py
FastAPI app με:
1. `POST /telnyx` — webhook, επιστρέφει TeXML που κατευθύνει τον ήχο στο WebSocket
2. `WebSocket /media-stream` — διαβάζει πρώτα το Telnyx `start` event (παίρνει `stream_id`, `call_control_id`, `codec`), δημιουργεί `TelnyxFrameSerializer` + `FastAPIWebsocketTransport`, τρέχει pipecat pipeline
3. Pipeline: `transport.input() → stt → tma_in → llm → tts → transport.output() → tma_out`
4. `on_client_connected` handler: στέλνει greeting μήνυμα στο LLM

### .env (3 keys μόνο)
```
TELNYX_API_KEY=...
OPENAI_API_KEY=...
DEEPGRAM_API_KEY=...
```

### requirements.txt
```
pipecat-ai[deepgram,openai,websocket]==1.2.1
python-dotenv==1.2.2
fastapi[standard]>=0.136.0
uvicorn>=0.47.0
```

## Κόστος ανά λεπτό
| Υπηρεσία | Κόστος |
|---|---|
| Deepgram Nova-2 STT | $0.0059 |
| GPT-4o-mini LLM | $0.0003 |
| OpenAI TTS-1 | $0.0112 |
| Telnyx inbound | $0.003 |
| **Σύνολο** | **~$0.020** |

## Telnyx setup
- Βάλε webhook στο dashboard: `https://your-server/telnyx`
- Ελληνικός αριθμός: ~$2-5/μήνα

## Σημαντικές λεπτομέρειες
- Το `start` event από Telnyx διαβάζεται ΠΡΙΝ δημιουργηθεί ο transport (`await websocket.receive_text()`)
- `auto_hang_up=True` (default) — χρειάζεται `call_control_id` + `TELNYX_API_KEY`
- Χωρίς Daily.co, χωρίς DAILY_ROOM_URL/TOKEN

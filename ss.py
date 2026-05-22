import asyncio
import json
import os

import uvicorn
from dotenv import load_dotenv
from fastapi import FastAPI, Request, WebSocket
from fastapi.responses import Response

from pipecat.frames.frames import LLMMessagesAppendFrame, LLMRunFrame
from pipecat.pipeline.pipeline import Pipeline
from pipecat.pipeline.runner import PipelineRunner
from pipecat.pipeline.task import PipelineParams, PipelineTask
from pipecat.processors.aggregators.llm_context import LLMContext
from pipecat.processors.aggregators.llm_response_universal import LLMContextAggregatorPair
from pipecat.serializers.telnyx import TelnyxFrameSerializer
from pipecat.services.deepgram import DeepgramSTTService
from pipecat.services.openai.llm import OpenAILLMService
from pipecat.services.openai.tts import OpenAITTSService
from pipecat.transports.websocket.fastapi import FastAPIWebsocketParams, FastAPIWebsocketTransport

load_dotenv()

SYSTEM_PROMPT = """Είσαι η Σοφία, η εικονική ρεσεψιονίστ του παιδικού κομμωτηρίου "Dancing Scissors".

Κανόνες συμπεριφοράς:
- Απαντάς ΠΑΝΤΑ στα ελληνικά, εκτός αν ο πελάτης μιλήσει άλλη γλώσσα — τότε προσαρμόζεσαι.
- Είσαι ζεστή, φιλική και σύντομη. Ο τόνος σου είναι χαρούμενος γιατί μιλάς σε γονείς παιδιών.
- ΔΕΝ εφευρίσκεις τιμές ή ωράρια που δεν γνωρίζεις — λες ότι θα επικοινωνήσει κάποιος μαζί τους.
- Για ραντεβού, ακυρώσεις και τιμές παραπέμπεις στο κατάστημα: 210-8001779.

Πληροφορίες επιχείρησης:
- Όνομα: Dancing Scissors — το πρώτο κομμωτήριο αποκλειστικά για παιδιά και εφήβους στην Ελλάδα
- Διεύθυνση: Τατοΐου 102, Νέα Ερυθραία, Αθήνα
- Τηλέφωνο: 210-8001779
- Υποκατάστημα: Golden Hall (Μαρούσι)

Υπηρεσίες:
- Κούρεμα παιδιών και εφήβων σε παιχνιδιάρικο περιβάλλον
- Τα παιδιά παίζουν PlayStation, βλέπουν ταινίες DVD και ζωγραφίζουν κατά τη διάρκεια του κουρέματος
- Παιδικά πάρτι με περιποίηση νυχιών, φυσικές μάσκες ομορφιάς και επιμελημένα χτενίσματα
- Όλα τα προϊόντα είναι 100% οργανικά και μη τοξικά — ασφαλή για το ευαίσθητο παιδικό δέρμα
- Κομμωτές εκπαιδευμένοι ειδικά για παιδιά

Ξεκίνα πάντα με ένα ζεστό καλωσόρισμα όταν ο πελάτης συνδεθεί."""

app = FastAPI()


@app.post("/telnyx")
async def telnyx_webhook(request: Request):
    """Telnyx webhook — καλείται για κάθε εισερχόμενη κλήση."""
    host = request.headers.get("host", "localhost")
    scheme = "wss" if request.headers.get("x-forwarded-proto") == "https" else "ws"

    texml = f"""<?xml version="1.0" encoding="UTF-8"?>
<Response>
  <Connect>
    <Stream url="{scheme}://{host}/media-stream" />
  </Connect>
</Response>"""
    return Response(content=texml, media_type="application/xml")


@app.websocket("/media-stream")
async def media_stream(websocket: WebSocket):
    await websocket.accept()

    # Το πρώτο μήνυμα από το Telnyx είναι το 'start' event με metadata κλήσης.
    raw = await websocket.receive_text()
    start_event = json.loads(raw)
    stream_info = start_event.get("start", {})
    stream_id = stream_info.get("stream_id", "")
    call_control_id = stream_info.get("call_control_id", "")
    codec = stream_info.get("codec", "PCMU")

    serializer = TelnyxFrameSerializer(
        stream_id=stream_id,
        outbound_encoding=codec,
        inbound_encoding=codec,
        call_control_id=call_control_id,
        api_key=os.getenv("TELNYX_API_KEY"),
    )

    transport = FastAPIWebsocketTransport(
        websocket=websocket,
        params=FastAPIWebsocketParams(
            serializer=serializer,
            audio_in_enabled=True,
            audio_out_enabled=True,
        ),
    )

    stt = DeepgramSTTService(
        api_key=os.getenv("DEEPGRAM_API_KEY"),
        language="el",
        model="nova-2",
    )

    llm = OpenAILLMService(
        api_key=os.getenv("OPENAI_API_KEY"),
        model="gpt-4o-mini",
    )

    tts = OpenAITTSService(
        api_key=os.getenv("OPENAI_API_KEY"),
        voice="nova",
        model="tts-1",
    )

    context = LLMContext(messages=[{"role": "system", "content": SYSTEM_PROMPT}])
    aggregators = LLMContextAggregatorPair(context)

    pipeline = Pipeline([
        transport.input(),
        stt,
        aggregators.user(),
        llm,
        tts,
        transport.output(),
        aggregators.assistant(),
    ])

    task = PipelineTask(pipeline, PipelineParams(allow_interruptions=True))

    @transport.event_handler("on_client_connected")
    async def on_client_connected(transport, websocket):
        await task.queue_frames([
            LLMMessagesAppendFrame(messages=[{
                "role": "user",
                "content": "[Ο πελάτης μόλις συνδέθηκε. Χαιρέτησέ τον σύντομα και ρώτα πώς μπορείς να βοηθήσεις.]",
            }]),
            LLMRunFrame(),
        ])

    runner = PipelineRunner()
    await runner.run(task)


if __name__ == "__main__":
    print("✓ AI Ρεσεψιόν 'Σοφία' — http://0.0.0.0:8000")
    print("  POST /telnyx       → webhook για εισερχόμενες κλήσεις")
    print("  WS   /media-stream → media stream Telnyx")
    uvicorn.run(app, host="0.0.0.0", port=8000)

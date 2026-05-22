import asyncio
import os
from dotenv import load_dotenv

from pipecat.pipeline.pipeline import Pipeline
from pipecat.pipeline.runner import PipelineRunner
from pipecat.pipeline.task import PipelineParams, PipelineTask
from pipecat.services.deepgram import DeepgramSTTService
from pipecat.services.openai import OpenAILLMService, OpenAITTSService
from pipecat.transports.services.daily import DailyParams, DailyTransport
from pipecat.processors.aggregators.llm_response import (
    LLMAssistantResponseAggregator,
    LLMUserResponseAggregator,
)
from pipecat.frames.frames import LLMMessagesFrame

load_dotenv()

HOTEL_SYSTEM_PROMPT = """Είσαι η Σοφία, η εικονική ρεσεψιονίστ του ξενοδοχείου "Αιγαίο Μπλε" στη Σαντορίνη.

Κανόνες συμπεριφοράς:
- Απαντάς ΠΑΝΤΑ στα ελληνικά, εκτός αν ο πελάτης μιλήσει άλλη γλώσσα — τότε προσαρμόζεσαι.
- Είσαι ζεστή, επαγγελματική και σύντομη. Μία πρόταση αρκεί όταν η απάντηση είναι απλή.
- ΔΕΝ εφευρίσκεις αριθμούς δωματίων ή τιμές που δεν γνωρίζεις — λες ότι θα συνδέσεις τον πελάτη με τον αρμόδιο.

Πληροφορίες ξενοδοχείου:
- Ώρα check-in: 15:00 | Check-out: 11:00
- Πισίνα: ανοιχτή 08:00–22:00
- Εστιατόριο "Καλντέρα": πρωινό 07:30–10:30, δείπνο 19:00–23:00
- Σπα & wellness: κατόπιν ραντεβού (εσωτερική γραμμή 210)
- Δωρεάν transfer από/προς αεροδρόμιο Σαντορίνης (JTR) — απαιτείται κράτηση 24ω πριν
- Για κρατήσεις δωματίων, ακυρώσεις και ειδικά αιτήματα: παραπέμπεις στο reception@aigaio-blue.gr ή εσωτερική γραμμή 0.

Ξεκίνα πάντα με ένα σύντομο καλωσόρισμα όταν ο πελάτης συνδεθεί."""


async def main():
    # --- TRANSPORT ---
    # Όταν έχεις Daily room URL και token, αυτό συνδέει τη γραμμή SIP/τηλεφώνου.
    # Βάλε DAILY_ROOM_URL και DAILY_TOKEN στο .env σου.
    transport = DailyTransport(
        room_url=os.getenv("DAILY_ROOM_URL"),
        token=os.getenv("DAILY_TOKEN"),
        bot_name="Σοφία | AI Ρεσεψιόν",
        params=DailyParams(
            audio_in_enabled=True,
            audio_out_enabled=True,
        )
    )

    # --- SERVICES ---
    stt = DeepgramSTTService(
        api_key=os.getenv("DEEPGRAM_API_KEY"),
        language="el",          # Ελληνικά ως προεπιλογή
        model="nova-2",
    )

    llm = OpenAILLMService(
        api_key=os.getenv("OPENAI_API_KEY"),
        model="gpt-4o-mini",
    )

    tts = OpenAITTSService(
        api_key=os.getenv("OPENAI_API_KEY"),
        voice="nova",           # "nova" ακούγεται πιο φυσικό για ελληνική γυναικεία φωνή
        model="tts-1",
    )

    # --- ΜΝΗΜΗ ΣΥΝΟΜΙΛΙΑΣ ---
    messages = [{"role": "system", "content": HOTEL_SYSTEM_PROMPT}]
    tma_in  = LLMUserResponseAggregator(messages)
    tma_out = LLMAssistantResponseAggregator(messages)

    # --- PIPELINE ---
    pipeline = Pipeline([
        transport.input(),   # Εισερχόμενος ήχος από τον καλούντα
        stt,                 # Speech-to-Text (Deepgram)
        tma_in,              # Αποθηκεύει αυτό που είπε ο χρήστης στη μνήμη
        llm,                 # Παράγει απάντηση (GPT-4o-mini)
        tts,                 # Text-to-Speech (OpenAI)
        transport.output(),  # Στέλνει τον ήχο πίσω στον καλούντα
        tma_out,             # Αποθηκεύει την απάντηση στη μνήμη
    ])

    task = PipelineTask(pipeline, PipelineParams(allow_interruptions=True))

    # --- ΑΡΧΙΚΟΣ ΧΑΙΡΕΤΙΣΜΟΣ ---
    # Μόλις συνδεθεί ο πρώτος συμμετέχων, η Σοφία παίρνει πρωτοβουλία και χαιρετά.
    @transport.event_handler("on_first_participant_joined")
    async def on_first_participant_joined(transport, participant):
        greeting_messages = messages + [
            {
                "role": "user",
                "content": "[Ο πελάτης μόλις συνδέθηκε. Χαιρέτησέ τον συντομα και ρώτα πώς μπορείς να βοηθήσεις.]"
            }
        ]
        await task.queue_frames([LLMMessagesFrame(greeting_messages)])

    # --- ΕΚΤΕΛΕΣΗ ---
    runner = PipelineRunner()
    print("✓ Η AI Ρεσεψιόν 'Σοφία' είναι έτοιμη — αναμένει κλήσεις...")
    await runner.run(task)


if __name__ == "__main__":
    asyncio.run(main())

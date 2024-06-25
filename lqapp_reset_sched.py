from app import create_app
from app.tasks import reset_processor_schedule


app = create_app()
app.app_context().push()

reset_processor_schedule()

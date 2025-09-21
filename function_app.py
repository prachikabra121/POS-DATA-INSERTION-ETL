import logging
import traceback
from main import main
import azure.functions as func
from typing import Optional
from email_sender import send_mail
from daily_cleanup import delete_old_data
app = func.FunctionApp()

@app.timer_trigger(
    schedule="45 21 * * *",
    arg_name="myTimer",
    run_on_startup=False,
    use_monitor=False
)
def timer_trigger_Daily(myTimer: func.TimerRequest) -> None:
    """
    Azure Timer Function that runs daily at 21:45.
    Includes enhanced error handling and logging.
    
    Args:
        myTimer (func.TimerRequest): Timer trigger input binding
    """
    function_name = "timer_trigger_Daily"
    logging.info(f'Starting {function_name} execution')
    
    if myTimer.past_due:
        logging.warning(f'{function_name} is running late!')
        
    try:
        # Log start time and function details
        logging.info(f'{function_name}: Initiating main() function')
        
        # Execute main function
        result = main()
        
        # Log successful completion
        logging.info(f'{function_name}: Completed successfully. Result: {result}')
        
    except Exception as e:
        # Enhanced error logging
        error_msg = f"Critical error in {function_name}: {str(e)}"
        stack_trace = traceback.format_exc()
        
        logging.error(error_msg)
        logging.error(f"Stack trace:\n{stack_trace}")
        
        # Re-raise the exception to ensure the function is marked as failed
        raise


@app.timer_trigger(schedule="30 0 * * *", arg_name="dailyCleanupTimer", run_on_startup=False, use_monitor=False)
def timer_trigger_daily_cleanup(dailyCleanupTimer: func.TimerRequest) -> None:
    if dailyCleanupTimer.past_due:
        logging.info('The daily cleanup timer is past due!')

    try:
        delete_old_data()  # Deletes records older than the last 3 days from the database
        logging.info('Daily cleanup completed successfully.')
    except Exception as e:
        logging.error(f"Error during daily cleanup: {e}")
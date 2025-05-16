from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# ===----------------------------------------------------------------------===#
# Silly LinkedIn Test DAG                                                     #
# This Dag can be used as a simple healthcheck test dag  (e.g migrations)     #
#                                                                             #
# Author: Walter J.T.V                                                        #          
# ===----------------------------------------------------------------------===#

def say_hello():
    print("🌟 *Absolutely THRILLED* to announce that we are now *officially leveraging the power of Airflow*! 🚀"
          " This is the *turning point* of our *data-driven journey* as we *pivot* toward *unimaginable scalability* and *unbreakable operational excellence*!"
          " Massive shoutout to the *visionary leadership* and the *relentless drive* from our *high-performing team* who made this happen! #Innovation #DataEngineering #Grindset #WinterArc #LetsGo")

def run_infinite_meeting():
    print("🧑‍💻 Kicking off another *endless strategy meeting* to ensure that every *stakeholder* is aligned toward our *unreachable KPIs*... *No time for sleep, only progress* 😴")

def deploy_with_uncertainty():
    print("🚀 *Deploying with zero testing* and a *positive mindset*! We're moving fast and breaking things! *Failure is a lesson, not an obstacle* 💪 #LetsFailForward")

def say_goodbye():
    print("📢 After an *incredible journey* with Airflow, it’s time to *pivot* once again and embark on *new opportunities*. *Onward to the next grind! We’re just getting started.* 💼 #GrindNeverStops")

def check_system_health():
    print("🩺 *System Health Check*... Everything is *green*, but can we push it further? 😎")

def failure_mode():
    print("❌ *Something went wrong*... but *failure is part of the grind*... Time to *learn* and come back stronger!")

def post_on_linkedin(status):
    print(f"💼 *Posting on LinkedIn*: {status}")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
}

with DAG(
    dag_id='dag_silly_linkedin',
    default_args=default_args,
    schedule='@hourly',
    catchup=False,
) as dag:

    # Start the DAG with a *thrilling announcement*
    hello_task = PythonOperator(
        task_id='thrilled_to_announce',
        python_callable=say_hello,
    )

    # Create a health check task for validation
    health_check_task = PythonOperator(
        task_id='health_check',
        python_callable=check_system_health,
    )

    # The task responsible for holding a 'never-ending' meeting
    meeting_task = PythonOperator(
        task_id='infinite_sync_meeting',
        python_callable=run_infinite_meeting,
    )

    # Task to deploy with zero confidence
    deploy_task = PythonOperator(
        task_id='deploy_with_uncertainty',
        python_callable=deploy_with_uncertainty,
    )

    # Task for *failure mode* when the system health check fails
    failure_task = PythonOperator(
        task_id='failure_mode',
        python_callable=failure_mode,
    )

    # Task to simulate *LinkedIn post* (for both successful and failure scenarios)
    linkedin_post_success = PythonOperator(
        task_id='linkedin_post_success',
        python_callable=lambda: post_on_linkedin(
            "💼 *WE'RE ON A MISSION* to change the *game* with Airflow, and nothing will stop us! 🚀"
            " *This is just the beginning* of the *Winter Arc*—a journey of *unwavering focus* and *growth mindset*."
            " *Grind now, shine later* #Airflow #DataEngineering #Grindset #WinterArc #LetsDominate"
        ),
    )

    linkedin_post_failure = PythonOperator(
        task_id='linkedin_post_failure',
        python_callable=lambda: post_on_linkedin(
            "💼 *Woke up to failure*, but we’re *built for this* 💪. *It's all part of the grind*—*failure doesn’t define us*, it refines us."
            " *Embrace the grind* and watch us come back stronger. #FailureIsThePathToSuccess #WinterArc #GrindNeverStops"
        ),
    )

    # Goodbye task to end the process
    goodbye_task = PythonOperator(
        task_id='bittersweet_farewell',
        python_callable=say_goodbye,
    )

    # Setting up the task dependencies in a complex way
    hello_task >> health_check_task  
    health_check_task >> deploy_task

    # If health check passes, deploy and post on LinkedIn with success message
    deploy_task >> linkedin_post_success >> goodbye_task

    # If health check fails, post on LinkedIn with failure message
    health_check_task >> failure_task >> linkedin_post_failure >> goodbye_task

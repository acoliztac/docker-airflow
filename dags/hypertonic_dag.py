from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import json
from os.path import exists

patient_list_filename = "Hypertensive_Patient_list.json"


def task_welcome():
    print(
        "Hi there! In next few seconds this DAG will check client's measures, stored in the document and calculate how many person will die in the nearest future")


def init_patients_list():
    global patient_list_filename
    if not exists(patient_list_filename):
        patients = {
            "Геннадий Волков": ["130/90", "140/94"],
            "Ирина Хакамада": ["160/110"],
            "Гай Юлий Цезарь": ["150/80"],
            "Авраам Линкольн": ["150/92", "140/90", "138/84"],
            "Роман Жуков": ["140/94", "140/90", "120/80"],
        }
        with open(patient_list_filename, 'w', encoding='utf-8') as w:
            json.dump(patients, w, indent=2)
            new_json = json.dumps(patients, indent=2)
            print("\n********************\n------> FILE SAVED CONTENT: <------\n{}\n********************\n".format(new_json))


def calculate_hypertensive():
    global patient_list_filename

    with open(patient_list_filename) as f:
        patients = json.load(f)
        new_json = json.dumps(patients, indent=2)
        print("\n********************\n------> FILE LOADED CONTENT: <------\n{}\n********************\n".format(new_json))

    ill_patients = []
    for patient in patients:
        patient_name = patient
        patient_measures = patients[patient]

        is_client_ill = False

        total = [0, 0]  # [systolic, diatolic]
        for measure in patient_measures:
            measure_arr = measure.split("/")
            systolic = int(measure_arr[0])
            diastolic = int(measure_arr[1])

            total[0] += systolic
            total[1] += diastolic

            if systolic >= 180 and diastolic >= 110:
                is_client_ill = True

        average = [0, 0]
        measure_cnt = len(patient_measures)
        if measure_cnt > 1:
            average[0] = total[0] / measure_cnt
            average[1] = total[1] / measure_cnt

            if average[0] >= 140 or average[1] >= 90:
                is_client_ill = True

        if is_client_ill:
            ill_patients.append(patient_name)

    print(f"{ill_patients} близки к смерти")

    with open("Hypertensive_Death-list.json", 'w', encoding='utf-8') as w:
        json.dump(ill_patients, w, ensure_ascii=False)


with DAG('python_dag', description='Python DAG', schedule_interval='*/1 * * * *', start_date=datetime(2018, 11, 1),
         catchup=False) as dag:
    task_1 = PythonOperator(
        task_id='greeting_task',
        python_callable=task_welcome
    )
    task_2 = PythonOperator(
        task_id='init_patient_list_task',
        python_callable=init_patients_list
    )
    task_3 = PythonOperator(
        task_id='check_ill_patients_task',
        python_callable=calculate_hypertensive
    )

    task_1 >> task_2 >> task_3

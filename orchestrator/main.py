import pandas as pd
import redis
import json
import os
import time
import uuid
from datetime import datetime

# Configuration Redis
redis_client = redis.Redis(host=os.environ.get('REDIS_HOST', 'redis'), port=6379, db=0)

def load_data(filepath):
    """Charge les données depuis un fichier CSV."""
    print(f"Chargement des données depuis {filepath}")
    return pd.read_csv(filepath)

def split_data(df, num_workers):
    """Divise les données en chunks pour les workers."""
    chunk_size = len(df) // num_workers + (1 if len(df) % num_workers > 0 else 0)
    chunks = []
    
    for i in range(0, len(df), chunk_size):
        chunk = df.iloc[i:i+chunk_size]
        chunks.append(chunk)
    
    print(f"Données divisées en {len(chunks)} chunks")
    return chunks

def distribute_tasks(chunks, job_id):
    """Distribue les chunks aux workers via Redis."""
    task_ids = []
    
    for i, chunk in enumerate(chunks):
        task_id = f"task:{job_id}:{i}"
        # Conversion en JSON et stockage dans Redis
        redis_client.set(task_id, chunk.to_json(orient='records'))
        # Publication pour traitement
        redis_client.lpush('task_queue', task_id)
        task_ids.append(task_id)
    
    # Stockage du nombre total de tâches pour ce job
    redis_client.set(f"job:{job_id}:tasks_count", len(task_ids))
    print(f"Tâches distribuées, job_id: {job_id}, {len(task_ids)} tâches créées")
    
    return task_ids

def monitor_progress(job_id):
    """Surveille l'avancement du traitement."""
    total_tasks = int(redis_client.get(f"job:{job_id}:tasks_count"))
    
    while True:
        # Compte les tâches terminées
        completed_tasks = redis_client.scard(f"job:{job_id}:completed_tasks")
        
        print(f"Progression: {completed_tasks}/{total_tasks} tâches terminées")
        
        if completed_tasks == total_tasks:
            # Notification à l'aggregator que toutes les tâches sont terminées
            redis_client.publish('tasks_completed', job_id)
            print(f"Toutes les tâches du job {job_id} sont terminées")
            break
        
        time.sleep(2)

def main():
    # Identifiant unique pour ce job
    job_id = str(uuid.uuid4())
    
    # Configurations
    data_path = os.environ.get('DATA_PATH', '/data/transactions_autoconnect.csv')
    num_workers = int(os.environ.get('NUM_WORKERS', 3))
    
    # 1. Charger les données
    data = load_data(data_path)
    
    # 2. Diviser les données
    chunks = split_data(data, num_workers)
    
    # 3. Distribuer les tâches
    distribute_tasks(chunks, job_id)
    
    # 4. Surveiller l'avancement
    monitor_progress(job_id)

if __name__ == "__main__":
    main()
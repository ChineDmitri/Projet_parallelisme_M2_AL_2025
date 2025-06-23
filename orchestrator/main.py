import pandas as pd
import redis
import json
import os
import time
import uuid
from datetime import datetime
import threading

# Configuration Redis
redis_client = redis.Redis(host=os.environ.get('REDIS_HOST', 'redis'), port=6379, db=0)

def load_data(filepath):
    """Charge les données depuis un fichier CSV."""
    start_time = time.time()
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Début du chargement des données depuis {filepath}")
    
    df = pd.read_csv(filepath)
    
    end_time = time.time()
    duration = end_time - start_time
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Chargement terminé - {len(df)} lignes chargées en {duration:.2f}s")
    
    return df

def split_data(df, num_workers):
    """Divise les données en chunks pour les workers."""
    start_time = time.time()
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Début de la division des données en {num_workers} chunks")
    
    chunk_size = len(df) // num_workers + (1 if len(df) % num_workers > 0 else 0)
    chunks = []
    
    for i in range(0, len(df), chunk_size):
        chunk = df.iloc[i:i+chunk_size]
        chunks.append(chunk)
    
    end_time = time.time()
    duration = end_time - start_time
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Division terminée - {len(chunks)} chunks créés en {duration:.2f}s")
    
    return chunks

def distribute_tasks(chunks, job_id):
    """Distribue les chunks aux workers via Redis."""
    start_time = time.time()
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Début de la distribution des tâches (job_id: {job_id})")
    
    task_ids = []
    
    for i, chunk in enumerate(chunks):
        task_start = time.time()
        task_id = f"task:{job_id}:{i}"
        # Conversion en JSON et stockage dans Redis
        redis_client.set(task_id, chunk.to_json(orient='records'))
        # Publication pour traitement
        redis_client.lpush('task_queue', task_id)
        task_ids.append(task_id)
        
        task_duration = time.time() - task_start
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Tâche {task_id} distribuée ({len(chunk)} lignes) en {task_duration:.3f}s")
    
    # Stockage du nombre total de tâches pour ce job
    redis_client.set(f"job:{job_id}:tasks_count", len(task_ids))
    
    end_time = time.time()
    duration = end_time - start_time
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Distribution terminée - {len(task_ids)} tâches créées en {duration:.2f}s")
    
    return task_ids

def monitor_progress(job_id):
    """Surveille l'avancement du traitement."""
    start_time = time.time()
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Début du monitoring du job {job_id}")
    
    total_tasks = int(redis_client.get(f"job:{job_id}:tasks_count"))
    last_completed = 0
    
    while True:
        # Compte les tâches terminées
        completed_tasks = redis_client.scard(f"job:{job_id}:completed_tasks")
        
        # Log uniquement si le nombre de tâches terminées a changé
        if completed_tasks != last_completed:
            elapsed_time = time.time() - start_time
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Progression: {completed_tasks}/{total_tasks} tâches terminées (temps écoulé: {elapsed_time:.1f}s)")
            last_completed = completed_tasks
        
        if completed_tasks == total_tasks:
            # Notification à l'aggregator que toutes les tâches sont terminées
            redis_client.publish('tasks_completed', job_id)
            end_time = time.time()
            total_duration = end_time - start_time
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Toutes les tâches du job {job_id} sont terminées - Durée totale de traitement: {total_duration:.2f}s")
            break
        
        time.sleep(2)

def run_orchestration(job_id=None):
    """Execute l'orchestration complète des données."""
    if not job_id:
        job_id = str(uuid.uuid4())
    
    overall_start_time = time.time()
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] === DÉBUT DE L'ORCHESTRATION ===")
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 🚀 Job ID: {job_id}")
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 🕐 Temps de début: {datetime.now().isoformat()}")
    
    # Mise à jour du statut et temps de début
    redis_client.set(f"job:{job_id}:status", "running")
    redis_client.set(f"job:{job_id}:orchestration_start", str(overall_start_time))
    redis_client.set(f"job:{job_id}:start_timestamp", datetime.now().isoformat())
    
    # Configurations
    data_path = os.environ.get('DATA_PATH', '/data/transactions_autoconnect.csv')
    num_workers = int(os.environ.get('NUM_WORKERS', 3))
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ⚙️  Configuration: {num_workers} workers, données: {data_path}")
    
    try:
        # Métriques par étape avec timestamps
        step_times = {}
        step_timestamps = {}
        
        # 1. Charger les données
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ⏱️  ÉTAPE 1/4: Chargement des données")
        step_start = time.time()
        step_timestamps['data_loading_start'] = datetime.now().isoformat()
        
        data = load_data(data_path)
        
        step_times['data_loading'] = time.time() - step_start
        step_timestamps['data_loading_end'] = datetime.now().isoformat()
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ✅ ÉTAPE 1/4 terminée en {step_times['data_loading']:.2f}s - {len(data)} lignes chargées")
        redis_client.set(f"job:{job_id}:step_times", json.dumps(step_times))
        redis_client.set(f"job:{job_id}:step_timestamps", json.dumps(step_timestamps))
        
        # 2. Diviser les données
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ⏱️  ÉTAPE 2/4: Division des données")
        step_start = time.time()
        step_timestamps['data_splitting_start'] = datetime.now().isoformat()
        
        chunks = split_data(data, num_workers)
        
        step_times['data_splitting'] = time.time() - step_start
        step_timestamps['data_splitting_end'] = datetime.now().isoformat()
        avg_chunk_size = len(data) // len(chunks) if chunks else 0
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ✅ ÉTAPE 2/4 terminée en {step_times['data_splitting']:.2f}s - {len(chunks)} chunks créés (taille moyenne: {avg_chunk_size} lignes)")
        redis_client.set(f"job:{job_id}:step_times", json.dumps(step_times))
        redis_client.set(f"job:{job_id}:step_timestamps", json.dumps(step_timestamps))
        
        # 3. Distribuer les tâches
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ⏱️  ÉTAPE 3/4: Distribution des tâches")
        step_start = time.time()
        step_timestamps['task_distribution_start'] = datetime.now().isoformat()
        
        distribute_tasks(chunks, job_id)
        
        step_times['task_distribution'] = time.time() - step_start
        step_timestamps['task_distribution_end'] = datetime.now().isoformat()
        distribution_rate = len(chunks) / step_times['task_distribution'] if step_times['task_distribution'] > 0 else 0
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ✅ ÉTAPE 3/4 terminée en {step_times['task_distribution']:.2f}s - Débit: {distribution_rate:.1f} tâches/sec")
        redis_client.set(f"job:{job_id}:step_times", json.dumps(step_times))
        redis_client.set(f"job:{job_id}:step_timestamps", json.dumps(step_timestamps))
        
        # 4. Surveiller l'avancement
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ⏱️  ÉTAPE 4/4: Monitoring du traitement")
        step_start = time.time()
        step_timestamps['monitoring_start'] = datetime.now().isoformat()
        
        monitor_progress(job_id)
        
        step_times['monitoring'] = time.time() - step_start
        step_timestamps['monitoring_end'] = datetime.now().isoformat()
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ✅ ÉTAPE 4/4 terminée en {step_times['monitoring']:.2f}s")
        
        overall_end_time = time.time()
        total_duration = overall_end_time - overall_start_time
        step_times['total'] = total_duration
        step_timestamps['orchestration_end'] = datetime.now().isoformat()
        
        # Calculs des métriques de performance
        total_rows = len(data)
        throughput = total_rows / total_duration if total_duration > 0 else 0
        
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] === ORCHESTRATION TERMINÉE ===")
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 🏁 Durée totale: {total_duration:.2f}s")
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 📊 RAPPORT DÉTAILLÉ DES TEMPS:")
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]   📂 Chargement données: {step_times['data_loading']:.2f}s ({(step_times['data_loading']/total_duration*100):.1f}%) - {total_rows/step_times['data_loading']:.0f} lignes/sec")
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]   ✂️  Division données: {step_times['data_splitting']:.2f}s ({(step_times['data_splitting']/total_duration*100):.1f}%) - {len(chunks)/step_times['data_splitting']:.1f} chunks/sec")
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]   📤 Distribution tâches: {step_times['task_distribution']:.2f}s ({(step_times['task_distribution']/total_duration*100):.1f}%) - {len(chunks)/step_times['task_distribution']:.1f} tâches/sec")
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]   👀 Monitoring: {step_times['monitoring']:.2f}s ({(step_times['monitoring']/total_duration*100):.1f}%)")
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 📈 Performance globale:")
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]   • Débit total: {throughput:.0f} lignes/seconde")
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]   • Volume traité: {total_rows:,} lignes")
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]   • Parallélisme: {num_workers} workers")
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 🕐 Heure de fin: {datetime.now().isoformat()}")
        
        # Mise à jour du statut final avec toutes les métriques détaillées
        performance_metrics = {
            'total_rows': total_rows,
            'throughput': throughput,
            'avg_chunk_size': avg_chunk_size,
            'distribution_rate': distribution_rate,
            'num_workers': num_workers,
            'num_chunks': len(chunks)
        }
        
        redis_client.set(f"job:{job_id}:status", "completed")
        redis_client.set(f"job:{job_id}:duration", str(total_duration))
        redis_client.set(f"job:{job_id}:step_times", json.dumps(step_times))
        redis_client.set(f"job:{job_id}:step_timestamps", json.dumps(step_timestamps))
        redis_client.set(f"job:{job_id}:performance_metrics", json.dumps(performance_metrics))
        redis_client.set(f"job:{job_id}:completion_time", str(overall_end_time))
        redis_client.set(f"job:{job_id}:completion_timestamp", datetime.now().isoformat())
        
    except Exception as e:
        error_time = time.time()
        error_duration = error_time - overall_start_time
        error_timestamp = datetime.now().isoformat()
        
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ❌ ERREUR lors de l'orchestration: {str(e)}")
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ⏱️  Durée avant erreur: {error_duration:.2f}s")
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 🕐 Heure d'erreur: {error_timestamp}")
        
        redis_client.set(f"job:{job_id}:status", "failed")
        redis_client.set(f"job:{job_id}:error", str(e))
        redis_client.set(f"job:{job_id}:error_time", str(error_time))
        redis_client.set(f"job:{job_id}:error_timestamp", error_timestamp)
        redis_client.set(f"job:{job_id}:duration", str(error_duration))

def listen_for_triggers():
    """Écoute les messages Redis pour déclencher l'orchestration."""
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 🎧 Démarrage de l'écoute Redis sur le canal 'start_processing'")
    
    pubsub = redis_client.pubsub()
    pubsub.subscribe('start_processing')
    
    for message in pubsub.listen():
        if message['type'] == 'message':
            try:
                data = json.loads(message['data'])
                job_id = data.get('job_id')
                
                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 📡 Signal de traitement reçu pour job_id: {job_id}")
                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 🚀 Lancement de l'orchestration...")
                
                # Lancer l'orchestration dans un thread séparé
                thread = threading.Thread(target=run_orchestration, args=(job_id,))
                thread.daemon = True
                thread.start()
                
            except json.JSONDecodeError:
                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ⚠️  Message Redis invalide reçu")

def main():
    mode = os.environ.get('ORCHESTRATOR_MODE', 'listener')
    
    if mode == 'standalone':
        # Mode standalone - exécute directement l'orchestration
        run_orchestration()
    else:
        # Mode listener - écoute les signaux Redis
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Orchestrator en mode écoute Redis")
        listen_for_triggers()

if __name__ == "__main__":
    main()
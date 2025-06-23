from flask import Flask, jsonify, request
import redis
import json
import os
import uuid
from datetime import datetime
import time

app = Flask(__name__)

# Configuration Redis
redis_client = redis.Redis(host=os.environ.get('REDIS_HOST', 'redis'), port=6379, db=0)

@app.route('/api/ca-mensuel', methods=['GET'])
def get_monthly_revenue():
    """API pour obtenir le chiffre d'affaires mensuel par ville."""
    # Paramètres optionnels
    ville = request.args.get('ville')
    mois = request.args.get('mois')  # Format: YYYY-MM
    
    # Récupération des derniers résultats
    results_json = redis_client.get('latest_results')
    
    if not results_json:
        return jsonify({"error": "Aucun résultat disponible"}), 404
    
    results = json.loads(results_json)
    ca_mensuel = results.get('ca_mensuel_ville', {})
    
    # Filtrage par ville si spécifié
    if ville:
        if ville in ca_mensuel:
            ca_mensuel = {ville: ca_mensuel[ville]}
        else:
            ca_mensuel = {}
    
    # Filtrage par mois si spécifié
    if mois:
        for city, months in ca_mensuel.items():
            if mois in months:
                ca_mensuel[city] = {mois: months[mois]}
            else:
                ca_mensuel[city] = {}
    
    return jsonify(ca_mensuel)

@app.route('/api/repartition', methods=['GET'])
def get_distribution():
    """API pour obtenir la répartition vente/location par ville."""
    # Paramètre optionnel
    ville = request.args.get('ville')
    
    # Récupération des derniers résultats
    results_json = redis_client.get('latest_results')
    
    if not results_json:
        return jsonify({"error": "Aucun résultat disponible"}), 404
    
    results = json.loads(results_json)
    
    # Selon le paramètre "format", renvoyer soit les comptages bruts soit les pourcentages
    if request.args.get('format') == 'percentage':
        distribution = results.get('pourcentage_vente_location', {})
    else:
        distribution = results.get('repartition_vente_location', {})
    
    # Filtrage par ville si spécifié
    if ville:
        if ville in distribution:
            distribution = {ville: distribution[ville]}
        else:
            distribution = {}
    
    return jsonify(distribution)

@app.route('/api/top-modeles', methods=['GET'])
def get_top_models():
    """API pour obtenir les modèles les plus populaires par ville."""
    # Paramètre optionnel
    ville = request.args.get('ville')
    
    # Récupération des derniers résultats
    results_json = redis_client.get('latest_results')
    
    if not results_json:
        return jsonify({"error": "Aucun résultat disponible"}), 404
    
    results = json.loads(results_json)
    top_models = results.get('top_models', {})
    
    # Filtrage par ville si spécifié
    if ville:
        if ville in top_models:
            top_models = {ville: top_models[ville]}
        else:
            top_models = {}
    
    return jsonify(top_models)

@app.route('/api/process', methods=['POST'])
def trigger_processing():
    """API pour déclencher un nouveau traitement."""
    start_time = time.time()
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] === REQUÊTE POST REÇUE SUR /api/process ===")
    
    # Création d'un nouvel ID de job
    job_id = str(uuid.uuid4())
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 🆔 Job ID généré: {job_id}")
    
    try:
        # Test de connexion Redis
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 🔍 Test de connexion Redis...")
        redis_ping = redis_client.ping()
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ✅ Redis ping: {redis_ping}")
        
        # Vérification des abonnés au canal
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 🔍 Vérification des abonnés au canal 'start_processing'...")
        pubsub_channels = redis_client.pubsub_channels('start_processing')
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 📡 Canaux actifs: {pubsub_channels}")
        
        pubsub_numsub = redis_client.pubsub_numsub('start_processing')
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 👥 Nombre d'abonnés sur 'start_processing': {pubsub_numsub}")
        
        # Nettoyage des anciennes données de job si nécessaire
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 🧹 Nettoyage des anciennes données...")
        
        # Préparation du message
        message_data = {
            'job_id': job_id,
            'timestamp': datetime.now().isoformat(),
            'triggered_by': 'api',
            'data_path': os.environ.get('DATA_PATH', '/data/transactions_autoconnect.csv'),
            'num_workers': int(os.environ.get('NUM_WORKERS', 3))
        }
        
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 📝 Message à envoyer: {message_data}")
        
        # Publication d'un message pour déclencher le traitement dans l'orchestrator
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 📤 Envoi du signal de démarrage à l'orchestrator...")
        
        num_subscribers = redis_client.publish('start_processing', json.dumps(message_data))
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 📊 Signal publié vers {num_subscribers} abonnés")
        
        if num_subscribers == 0:
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ⚠️  ATTENTION: Aucun abonné trouvé sur le canal 'start_processing'!")
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 🔍 L'orchestrator n'est peut-être pas démarré ou n'écoute pas le bon canal")
        
        # Enregistrement du démarrage du job
        redis_client.set(f"job:{job_id}:status", "initiated")
        redis_client.set(f"job:{job_id}:start_time", str(start_time))
        redis_client.set(f"job:{job_id}:api_trigger_time", datetime.now().isoformat())
        
        duration = time.time() - start_time
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ✅ Signal envoyé avec succès en {duration:.3f}s")
        
        # Attendre un court instant pour voir si l'orchestrator répond
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ⏳ Attente de 2 secondes pour vérifier la réception...")
        time.sleep(2)
        
        # Vérifier si le statut a changé
        updated_status = redis_client.get(f"job:{job_id}:status")
        if updated_status:
            updated_status = updated_status.decode('utf-8')
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 📊 Statut après 2s: {updated_status}")
            if updated_status == "running":
                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 🎉 L'orchestrator a bien reçu le signal!")
            elif updated_status == "initiated":
                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ⚠️  L'orchestrator n'a pas encore traité le signal")
        
        return jsonify({
            "status": "processing_started",
            "job_id": job_id,
            "message": "Le traitement a été déclenché avec succès",
            "timestamp": datetime.now().isoformat(),
            "subscribers_notified": num_subscribers,
            "orchestrator_listening": num_subscribers > 0
        }), 202
        
    except Exception as e:
        error_duration = time.time() - start_time
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ❌ ERREUR lors du déclenchement: {str(e)} (durée: {error_duration:.3f}s)")
        
        return jsonify({
            "status": "error",
            "message": f"Erreur lors du déclenchement: {str(e)}",
            "timestamp": datetime.now().isoformat()
        }), 500

@app.route('/api/job/<job_id>/status', methods=['GET'])
def get_job_status(job_id):
    """API pour obtenir le statut et les métriques d'un job."""
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Demande de statut pour job_id: {job_id}")
    
    try:
        # Récupération du statut
        status = redis_client.get(f"job:{job_id}:status")
        if not status:
            return jsonify({"error": "Job non trouvé"}), 404
        
        status = status.decode('utf-8')
        
        # Récupération des métriques temporelles
        start_time = redis_client.get(f"job:{job_id}:start_time")
        duration = redis_client.get(f"job:{job_id}:duration")
        error = redis_client.get(f"job:{job_id}:error")
        
        # Calcul du temps écoulé si le job est en cours
        elapsed_time = None
        if start_time and status == "running":
            elapsed_time = time.time() - float(start_time.decode('utf-8'))
        
        # Progression des tâches
        tasks_count = redis_client.get(f"job:{job_id}:tasks_count")
        completed_tasks = redis_client.scard(f"job:{job_id}:completed_tasks")
        
        response = {
            "job_id": job_id,
            "status": status,
            "timestamp": datetime.now().isoformat()
        }
        
        if start_time:
            response["start_time"] = datetime.fromtimestamp(float(start_time.decode('utf-8'))).isoformat()
        
        if duration:
            response["total_duration"] = float(duration.decode('utf-8'))
        
        if elapsed_time:
            response["elapsed_time"] = elapsed_time
        
        if error:
            response["error"] = error.decode('utf-8')
        
        if tasks_count:
            response["progress"] = {
                "completed_tasks": completed_tasks,
                "total_tasks": int(tasks_count.decode('utf-8')),
                "percentage": round((completed_tasks / int(tasks_count.decode('utf-8'))) * 100, 2) if int(tasks_count.decode('utf-8')) > 0 else 0
            }
        
        return jsonify(response)
        
    except Exception as e:
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Erreur lors de la récupération du statut: {str(e)}")
        return jsonify({"error": f"Erreur: {str(e)}"}), 500

@app.route('/api/jobs', methods=['GET'])
def get_recent_jobs():
    """API pour obtenir la liste des jobs récents."""
    try:
        # Recherche des jobs dans Redis (pattern matching)
        job_keys = redis_client.keys("job:*:status")
        jobs = []
        
        for key in job_keys:
            job_id = key.decode('utf-8').split(':')[1]
            status = redis_client.get(key).decode('utf-8')
            
            start_time = redis_client.get(f"job:{job_id}:start_time")
            duration = redis_client.get(f"job:{job_id}:duration")
            
            job_info = {
                "job_id": job_id,
                "status": status
            }
            
            if start_time:
                job_info["start_time"] = datetime.fromtimestamp(float(start_time.decode('utf-8'))).isoformat()
            
            if duration:
                job_info["duration"] = float(duration.decode('utf-8'))
            
            jobs.append(job_info)
        
        # Trier par heure de début (plus récent en premier)
        jobs.sort(key=lambda x: x.get('start_time', ''), reverse=True)
        
        return jsonify({"jobs": jobs[:10]})  # Limite aux 10 derniers jobs
        
    except Exception as e:
        return jsonify({"error": f"Erreur: {str(e)}"}), 500

@app.route('/api/villes', methods=['GET'])
def get_cities():
    """API pour obtenir la liste des villes présentes dans les données."""
    # Récupération des derniers résultats
    results_json = redis_client.get('latest_results')
    
    if not results_json:
        return jsonify({"error": "Aucun résultat disponible"}), 404
    
    results = json.loads(results_json)
    cities = list(results.get('ca_mensuel_ville', {}).keys())
    
    return jsonify({"villes": cities})

@app.route('/api/debug/redis', methods=['GET'])
def debug_redis():
    """Endpoint de debug pour vérifier l'état de Redis."""
    try:
        info = {
            "redis_ping": redis_client.ping(),
            "redis_info": {
                "connected_clients": redis_client.info()['connected_clients'],
                "used_memory_human": redis_client.info()['used_memory_human'],
                "uptime_in_seconds": redis_client.info()['uptime_in_seconds']
            },
            "pubsub_channels": redis_client.pubsub_channels(),
            "start_processing_subscribers": redis_client.pubsub_numsub('start_processing')[0][1] if redis_client.pubsub_numsub('start_processing') else 0,
            "active_jobs": []
        }
        
        # Recherche des jobs actifs
        job_keys = redis_client.keys("job:*:status")
        for key in job_keys:
            job_id = key.decode('utf-8').split(':')[1]
            status = redis_client.get(key).decode('utf-8')
            if status in ['initiated', 'running']:
                info["active_jobs"].append({"job_id": job_id, "status": status})
        
        return jsonify(info)
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
from flask import Flask, jsonify, request
import redis
import json
import os
import uuid

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
    # Création d'un nouvel ID de job
    job_id = str(uuid.uuid4())
    
    # Publication d'un message pour déclencher le traitement
    redis_client.publish('start_processing', job_id)
    
    return jsonify({
        "status": "processing_started",
        "job_id": job_id
    })

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

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
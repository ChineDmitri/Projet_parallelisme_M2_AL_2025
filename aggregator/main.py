import redis
import json
import os
import time
from collections import defaultdict

# Configuration Redis
redis_client = redis.Redis(host=os.environ.get('REDIS_HOST', 'redis'), port=6379, db=0)

def merge_monthly_revenues(results_list):
    """Fusionne les chiffres d'affaires mensuels par ville."""
    merged = defaultdict(lambda: defaultdict(float))
    
    for result in results_list:
        ca_mensuel = result.get('ca_mensuel_ville', {})
        for city, months in ca_mensuel.items():
            for month, revenue in months.items():
                merged[city][month] += revenue
    
    return {city: dict(months) for city, months in merged.items()}

def merge_sales_rental_distribution(results_list):
    """Fusionne la répartition vente/location par ville."""
    merged = defaultdict(lambda: defaultdict(int))
    
    for result in results_list:
        distribution = result.get('repartition_vente_location', {})
        for city, types in distribution.items():
            for transaction_type, count in types.items():
                merged[city][transaction_type] += count
    
    return {city: dict(types) for city, types in merged.items()}

def merge_top_models(results_list):
    """Fusionne et recalcule les modèles les plus populaires."""
    # Fusionner tous les comptages
    all_counts = defaultdict(lambda: defaultdict(int))
    
    for result in results_list:
        top_models = result.get('top_models', {})
        for city, models in top_models.items():
            for model, count in models.items():
                all_counts[city][model] += count
    
    # Sélectionner les top 5 pour chaque ville
    top_models = {}
    for city, models in all_counts.items():
        # Trier par nombre et prendre les 5 premiers
        sorted_models = sorted(models.items(), key=lambda x: x[1], reverse=True)[:5]
        top_models[city] = dict(sorted_models)
    
    return top_models

def calculate_sales_percentage(distribution):
    """Calcule le pourcentage de ventes/locations par ville."""
    percentages = {}
    
    for city, types in distribution.items():
        total = sum(types.values())
        percentages[city] = {
            transaction_type: round((count / total) * 100, 2) 
            for transaction_type, count in types.items()
        }
    
    return percentages

def aggregate_job_results(job_id):
    """Agrège les résultats de toutes les tâches d'un job."""
    print(f"Agrégation des résultats pour le job {job_id}")
    
    # Récupérer les IDs de toutes les tâches terminées
    task_ids = redis_client.smembers(f"job:{job_id}:completed_tasks")
    
    # Récupérer les résultats de chaque tâche
    results_list = []
    for task_id in task_ids:
        task_id = task_id.decode('utf-8')
        result_json = redis_client.get(f"{task_id}:results")
        
        if result_json:
            results_list.append(json.loads(result_json))
    
    # Fusionner les résultats
    aggregated = {
        'ca_mensuel_ville': merge_monthly_revenues(results_list),
        'repartition_vente_location': merge_sales_rental_distribution(results_list),
        'top_models': merge_top_models(results_list)
    }
    
    # Calculs supplémentaires
    aggregated['pourcentage_vente_location'] = calculate_sales_percentage(
        aggregated['repartition_vente_location']
    )
    
    # Stocker les résultats agrégés
    redis_client.set('latest_results', json.dumps(aggregated))
    redis_client.set(f"job:{job_id}:aggregated_results", json.dumps(aggregated))
    
    print(f"Résultats agrégés pour le job {job_id}")
    return aggregated

def main():
    print("Aggregator démarré, en attente de notifications...")
    
    # S'abonner au canal de notification
    pubsub = redis_client.pubsub()
    pubsub.subscribe('tasks_completed')
    
    for message in pubsub.listen():
        if message['type'] == 'message':
            job_id = message['data'].decode('utf-8')
            aggregate_job_results(job_id)

if __name__ == "__main__":
    main()
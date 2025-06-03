import pandas as pd
import redis
import json
import os
import time

# Configuration Redis
redis_client = redis.Redis(host=os.environ.get('REDIS_HOST', 'redis'), port=6379, db=0)

def process_monthly_revenue_by_city(df):
    """Calcule le chiffre d'affaires mensuel par ville."""
    # Convertir les dates en format datetime
    df['date'] = pd.to_datetime(df['date'])
    
    # Créer une colonne année-mois
    df['month'] = df['date'].dt.strftime('%Y-%m')
    
    # Calcul du CA mensuel par ville
    result = df.groupby(['ville', 'month'])['prix'].sum().reset_index()
    
    # Transformation en format dictionnaire
    monthly_revenue = {}
    for _, row in result.iterrows():
        city = row['ville']
        month = row['month']
        revenue = float(row['prix'])
        
        if city not in monthly_revenue:
            monthly_revenue[city] = {}
        
        monthly_revenue[city][month] = revenue
    
    return monthly_revenue

def calculate_sales_rental_distribution(df):
    """Calcule la répartition vente/location par ville."""
    # Comptage des transactions par ville et type
    result = df.groupby(['ville', 'type']).size().reset_index(name='count')
    
    # Transformation en format dictionnaire
    distribution = {}
    for _, row in result.iterrows():
        city = row['ville']
        transaction_type = row['type']
        count = int(row['count'])
        
        if city not in distribution:
            distribution[city] = {}
        
        distribution[city][transaction_type] = count
    
    return distribution

def find_top_models(df):
    """Détermine les 5 modèles les plus populaires par ville."""
    # Comptage des modèles par ville
    result = df.groupby(['ville', 'modele']).size().reset_index(name='count')
    
    # Tri et sélection des top 5
    top_models = {}
    for city in result['ville'].unique():
        city_data = result[result['ville'] == city]
        top_5 = city_data.sort_values('count', ascending=False).head(5)
        
        top_models[city] = {}
        for _, row in top_5.iterrows():
            top_models[city][row['modele']] = int(row['count'])
    
    return top_models

def process_task(task_id):
    """Traite une tâche spécifique."""
    print(f"Traitement de la tâche {task_id}")
    
    # Extraction de l'ID du job
    job_id = task_id.split(':')[1]
    
    # Récupération des données
    data_json = redis_client.get(task_id)
    if not data_json:
        print(f"Données introuvables pour la tâche {task_id}")
        return False
    
    # Conversion JSON en DataFrame
    df = pd.read_json(data_json.decode('utf-8'), orient='records')
    print(f"Tâche {task_id}: {len(df)} transactions à traiter")
    
    # Calculs
    results = {
        'ca_mensuel_ville': process_monthly_revenue_by_city(df),
        'repartition_vente_location': calculate_sales_rental_distribution(df),
        'top_models': find_top_models(df)
    }
    
    # Stockage des résultats dans Redis
    redis_client.set(f"{task_id}:results", json.dumps(results))
    
    # Marquer la tâche comme terminée
    redis_client.sadd(f"job:{job_id}:completed_tasks", task_id)
    
    print(f"Tâche {task_id} terminée avec succès")
    return True

def main():
    print("Worker démarré, en attente de tâches...")
    
    while True:
        # Récupération d'une tâche depuis la file d'attente
        task = redis_client.brpop('task_queue', timeout=1)
        
        if task:
            task_id = task[1].decode('utf-8')
            process_task(task_id)
        else:
            time.sleep(1)

if __name__ == "__main__":
    main()
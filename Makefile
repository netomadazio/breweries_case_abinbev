current_branch = 1.0.0

####################################################################################################
####################################################################################################
#####################    COMANDOS PARA CONFIGURAÇÃO DO AMBIENTE    #################################

build_airflow:
	docker build -f docker/airflow/Dockerfile -t airflow:$(current_branch) .

build_spark_apps:
	docker build -t spark_apps:$(current_branch) docker/spark_jobs

deploy_airflow:
	docker-compose -f services/airflow_services.yaml up -d --build

stop_airflow:
	docker-compose -f services/airflow_services.yaml down

watch_airflow:
	docker-compose -f services/airflow_services.yaml ps

deploy_spark:
	docker-compose -f services/spark_services.yaml up -d --build

stop_spark:
	docker-compose -f services/spark_services.yaml down

watch_spark:
	watch docker-compose -f services/spark_services.yaml ps

deploy_monitoring:
	docker compose -f services/monitoring_services.yml up -d
 
stop_monitoring:
	docker compose -f services/monitoring_services.yml down
 
watch_monitoring:
	watch docker compose -f services/monitoring_services.yml ps
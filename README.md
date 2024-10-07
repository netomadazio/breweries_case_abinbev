<h1 align="center"> Case AB Inbev</h1>

## Repositório destinado a construção do exercício proposto como forma de avaliação da empresa AB Inbev

<h4 align="center"> 
	🚧  AB Inbev Project 🚀 Concluído com pontos de melhorias..  🚧
</h4>

## Índice 

* [Título](#título)
* [Índice](#índice)
* [Descrição do Projeto](#descrição-do-projeto)
* [Tecnologias utilizadas](#tecnologias-utilizadas)
* [Arquitetura](#arquitetura)
* [Resolução](#resolução)
* [Pontos de melhorias](#pontos-de-melhorias)
* [Monitoring/Alerting](#Monitoring/Alerting)
* [Execução do projeto](#execução-do-projeto)
* [Conclusão](#conclusão)
* [Autor](#autor)

## Descrição do Projeto

Este projeto visa a resolução de um case proposto pela AB InBev como parte de um processo avaliativo. O objetivo é desenvolver uma pipeline utilizando o conceito de camadas bronze, silver e gold para processar dados extraídos de uma API.

### Etapas da Pipeline
- Camada Bronze:
    Os dados brutos são extraídos da API fornecida no case e armazenados na camada bronze, sendo inputados em seu formato bruto, sem quaisquer transformações.

- Camada Silver:
    A partir dos dados brutos da camada bronze, são aplicadas as transformações necessárias para garantir a qualidade e o formato apropriado. Os dados transformados são então gravados na camada silver, particionados por localidade.

- Camada Gold:
    Por fim, é criada uma view na camada gold contendo informações agregadas sobre o número de cervejarias, categorizadas por tipo e localidade. Esses dados são salvos em formato otimizado.

### Formatos de Armazenamento
Os dados processados nas camadas silver e gold devem ser salvos nos formatos Parquet ou Delta.

### Orquestração
Para a orquestração da pipeline, é necessário escolher uma ferramenta adequada, como Airflow, Luigi, Mage, entre outras.

### Considerações
Além das transformações de dados, o projeto deve contemplar:

- Tratamento de falhas durante o processo.
- Monitoramento da qualidade dos dados.
- Garantia de robustez e confiabilidade na pipeline de dados.


## Tecnologias utilizadas 
:hammer:

- `Github`: Ferramenta utilizada para versionamento do projeto e repositório;
- `Pyspark`: Linguagem para manipulação dos arquivos e dados necessários;
- `Python`: Linguagem para manipulação dos arquivos e dados necessários;
- `Docker`: Responsável por criar a imagem necessária para processamento da aplicação;
- `Docker-compose`: Realiza a orquestração dos contêiners;
- `Airflow`: Organiza o fluxo de trabalho;
- `Bucket S3 - AWS`: Storage destinado ao armazenamento do fluxo de dados;

## Arquitetura

O projeto é composto por duas DAG's, que serão detalhadas mais adianta, porém como forma de melhor descrição do processo, a arquitetura foi separada em dois desenhos mas podendo as duas estarém inseridas em um mesmo contexto.

![job_pandas](https://github.com/user-attachments/assets/22687081-09b5-4a90-b191-e6980f00e50d)
Na arquitetura podemos observar a instância do Airflow executada através de uma imagem Docker, no caso um docker compose que permite que o Airflow rode todos os seus microserviços necessários para seu funcionamento. Através de um PythonOperator executado pelo Airflow, é possível fazer uma requisição a API para consumo dos dados e inserí-los em um Bucker S3 na AWS (em camadas de acordo com contexto da aplicabilidade). Esse processo será detalhado mais adiante.

![job_spark](https://github.com/user-attachments/assets/f7d2909e-8825-4ef0-94d5-407f37479021)
Na aquitetura acima, podemos observar a diferença pela inserção do Spark no contexto. Diferentemente do fluxo anterior, nem todo processo será executado dentro do ambiente do Airflow, as taks que fazem uso do Spark (iremos detalhar melhor adiante), serão executadas através de um DockerOperator, isso permite isolar parte da aplicação para ser executada separadamente, assim evita possíveis problemas como conflitos de versões de bibliotecas e pacotes a serem instalados. Já a parte reponsável pela instância do Airflow e task que utiliza do PythonOperator, serão executadas igualmente a arquitetura anterior, como é o caso da requisição na API. Importante ressaltar que o ambiente Spark realiza a comunicação direta com Bucket S3, assim como o Airflow.

## Resolução

A primeira etapa para o desenvolvimento do projeto foi o desenho de como seria a arquitetura do processo de ponta a ponta, quais tecnologias seriam utilizadas, e que aspectos seriam necessários para cobrir as exigências do case proposto.

- Ferramenta de Orquestração:
    Optou-se pelo uso do Airflow, devido a sua robustes para orquestração de tarefas, incluindo programação de tarefas, gestão de dependências, monitoramento para erros, configuração de retries, além da sua interface amigável para monitoramento do processo.

- linguagem:
    Usou-se das linguagens python e pyspark para o desenvolvimento do case. Ambas as linguagens oferecem recursos importantes para acesso a API's e manipulação de dados, sendo ambas compatíveis com o airflow.

<div style="color:red;">
    OBSERVAÇÃO IMPORTANTE NO DESENVOLVIMENTO DO PROJETO

  Ao realizar o teste, observou-se que os dados apresentavam baixa volumetria, apenas 50 linhas, o que também permitiria o uso do pandas para realizar as transformações necessárias, sem que fosse preciso utilizar de uma ferramente de processamento distribuído como Spark. Durante o desenvolvimento pode-se notar vantagens e desvantagens na utilização de ambas as linguagens. Para manipulação de baixo volume de dados, bibliotecas como o pandas são muito satisfatórias e atendem perfeitamente os requisitos necessários, como tempo de processamento, libs, recursos, dentre outros, poŕem quando falamos de escrita e leitura de dados otimizados, inseridos em um datalake, como é o caso do projeto, onde os dados são inseridos em um Bucket S3, são necessárias maiores etapas para se obter o mesmo resultado comparadas ao Spark. O spark, por sua vez, oferece maior controle de recursos, ferramentas importantes como lazy evaluation, catalyst, processamento distribuído, dentre outros aspectos importantes, porém oferece maior complexidade em se provisionar o ambiente necessário para sua execução, o que pode gerar multiplos conflitos e falta de depedências, contudo após provisionado, oferece grande facilidade para transformações de dados, leitura e escrita de dados otimizados, não sendo necessárias tantas etapas.

  Realizou-se o processo em uma DAG fazendo uso da biblioteca pandas e em outra fazendo uso do Spark, assim é possível notar as diferenças entre os procesos. Embora funções como "to_pandas" e "spark.createDataFrame "pudessem ser utilizadas para combinar as soluções, optou-se por manter as abordagens separadas devido aos fatores já mencionados.
</div>

- Containerização: 
    Docker segue sendo um escolha interessante quando se trata de containers, já que é a maior ferramenta utilizada e facilmente integrada em diversos projetos, permitindo a modularização, garantindo a portabilidade da aplicação, criando um pacote com sua dependências, permitindo que seja executada tanto localmente quanto em ambientes de nuvem.

- Monitoramento e alertas:
    O projeto foi desenvolvido de forma a criar vários mecanismos para validações dos processos, utilizando do código e de ferramentas nativas do próprio Airflow, como configurações para retries, concurrency e envio de emails em caso de falha.

Desenhado a solução, seguiu-se com a resolução do case proposto. Descrevendo a solução por etapas:

- Task 1 (init_process_authentication):
    Criou-se essa task apenas como forma de simbolizar, que ambientes corporativos, sejam eles on premises ou em cloud services, costumam ser necessário a autenticação para acesso de leitura e escrita em datalakes, banco de dados, dentre outas coisas.

- Task 2 (extract_data_to_bronze):
    Nessa etapa fez-se a extração dos dados através da biblioteca requests do python, realizando as devidas tratativas para erros e fazendo o input desses dados na camada bronze do datalake, criando-se um histórico segmentado por ano, mês e dia. Sendo os dados salvos no seu formato bruto, sem nenhuma tratativa respeitando o conceito da camada bronze.

- Task 3 (transform_bronze_to_silver):
    A partir dos dados inputados na camada bronze, realizou-se a limpeza, transformações e conversão nos dados, afim de torná-los mais performáticos e facilitar em possíveis análises.

    Os dados foram lidos pelas bibliotecas pandas e pyspark, de acordo com a DAG em questão, as transformações realizadas contemplam:

    - Upper Case no nome das colunas:
        Essa operação visa evitar possíveis erros com cases senstive nas operações com dados
    - Upper case nos valores das colunas strings, exceto a coluna "id":
        Como no caso do nome das colunas, esse tipo de operação evita erros devido ao case senstive em operações realizadas com valores das colunas.
    - Eliminação de espaços nulos a esquerda e direita:
        Ação importante na mitigação de erros nas operações com dados, visto que nem sempre é possível perceber um espaço vazio a esquerda ou direita.
    - Tratamento para valores nulos:
        Necessário a adequação aos valores nulos, devido a também possibilidade de erros a manipular esse tipo de dados, atendo-se sempre a não ter perda de informação nas etapas de limpeza e tratamento dos dados.
    - Salvar em formato parquet particionado por localidade:
        Etapa final da etapa realizada, onde faz-se o input das informações na camada silver do datalake, particionando os dados por localidade, no caso optou-se por particionar por "COUNTRY", "STATE" e "CITY", assim é possível ler dados somente de determinado país, estado ou cidade.

- Task 4 (transform_silver_to_gold):
    Devido aos dados já terem sido limpos e tratados na etapa anterior, nessa faz-se necessário somente o check da integridade dos dados, e assim pode-se fazer as ações propostas nessa etapa.

    Também como na etapa anterior os dados foram lidos pelas bibliotecas pandas e pyspark, de acordo com a DAG em questão.

    - Inferiu-se o schema nos dados: 
        Essa etapa foi necessária devido à leitura de dados em uma camada particionada, que pode alterar o tipo de dado originalmente salvo, resultando em inconsistências nas etapas seguintes. Portanto, ao realizar a leitura, assegura-se que os dados mantenham o mesmo schema utilizado na task anterior.
      
    - Criação de view de quantidade de cervejaria, por tipo de localidade:
        Aplicou-se o metodo groupby nas colunas "BREWERY_TYPE", "COUNTRY", "STATE" e "CITY", assim podemos obter a visualização pedida no case proposto.

<span style="color:red">
    NOTA

  Ao se falar localidade, pode-se ter diferentes entendimentos, como somente o país, estado ou a cidade. Em um case real pode-se averiguar essa informação e chegar em uma solução ideal que irá atender a fins específicos, no caso fez-se a escolha que julgou-se melhor para o case.
</span>

Dentro das tasks aplicou-se em código, validações, tratativas para erros e exceções, estes que podem ser observados em logs do Airflow, assim como em caso de falha serem executados novamente.

Obs: O histórico dos dados é mantido somente na camada bronze, já as camadas silver e gold foram criadas de forma a fornecer uma visualização diárias desses dados, ou seja, irão disponibilizar somente os dados extraídos no dia da execução. Em um processo real, poderiam-se avaliar outros cenários onde seria possível também trazer uma visualização com histórico também nessas camadas, ou mesmo com um intervalo de dados melhor definido, como por exemplo os dados dos últimos 2 anos.

## Monitoring/Alerting:
Em um ambiente de produção, é fundamental implementar mecanismos eficazes de monitoramento e alertas para garantir a estabilidade, integridade, robustez e eficiência do pipeline. Abaixo estão algumas práticas que podem ser eficazes:

- Alertas em Tempo Real: 
    Configuração de notificações automáticas para falhas e erros via e-mail, SMS, ou ferramentas corporativas como Microsoft Teams ou Slack, garantindo uma resposta rápida a problemas.

- Dashboards de Monitoramento: 
    Uso de ferramentas como Grafana para criar dashboards que exibam o desempenho e o status das DAGs, proporcionando visibilidade contínua do pipeline.

No projeto atual como medida de monitoramento, criou-se uma camada de serviços para realizar telemetria e monitoramento de recursos da infraestrutura utilizados pelo sistema. Esses serviços são:
	- Prometheus: Serviço usado para coleta de logs e métricas utilizadas na telemetria e monitoramento;
	- Grafana: Serviço usado para visualização de logs e métricas coletadas pelo Prometheus, provendo dashboards para monitoramento;
	- Prometheus Agents e Exporters: Serviços responsáveis por coletar métricas de enviar para o Prometheus. Os agentes utilizados nesse trabalho foram:
		- Node exporter: Agente para coletar dados de telemetria do nó em específico.
		- Cadvisor: Agente para coletar dados de telemetria do docker.

Assim, após implementado a parte de monitoramento, tem-se:
	- Os exporters coletam essas métricas e as enviam para o Prometheus.
	- O Grafana consome essas métricas do Prometheus e as exibe em dashboards.

Nota:
	- Na parte de execução do sistema, será informado como implementar a solução no ambiente e visualizar os Dashboards.

- Otimização de Recursos: 
    Em ambientes como Kubernetes, ajustar o executor_config nas DAGs do Airflow para alocar recursos de maneira eficiente, evitando tanto o uso excessivo quanto o subdimensionamento de recursos, o que pode resultar em custos desnecessários ou falhas frequentes.

Essas são apenas algumas das soluções possíveis para implementar monitoramento e alertas em pipelines de dados.

## Pontos de melhorias
Pode-se destacar alguns pontos de melhoria para o desenvolvimento do projeto:

Provisionamento de ambientes segregados - de acordo com a branch em execução (ex: main, hml, prd), poderia-se realizar o deploy para diferentes instâncias, o que possibilitaria uma simulação de ambientes de desenvolvimento, homologação e produção.

Github Actions - implementação de ferramenta de CI/CD, também sendo possível validações como CodeQL, Lint, Flake, Blake nos "pushs" das branchs.

Catalogo de dados - assim teria-se melhor visualização dos dados inputados nas camadas bronze, silver e gold.

Testes Unitários - A realização de testes unitários para os códigos desenvolvidos, assim seria possível também validar a integridade das aplicações.

Great Expectations - em um caso real, poderia-se implementar uma validação em cima dos dados consumidos ou inseridos no datalake, de acordo com alguma regra de negócio estipulado pelos Stakeholders.

## Execução do projeto 
📁 

Para execução completa do projeto, seguir os passos descritos abaixo:

  - Clonar o repositório do projeto:
	```sh
	git clone https://github.com/netomadazio/breweries_case_abinbev.git

  - Verificação da branch:
	```sh
	git status

  - Caso não encontrar-se na branch main, realizar a mudança para branch principal:
    ```sh
    git checkout main

  - Executar os comandos abaixo, para que o Airflow tenha permissão nos diretórios em questão e também tenha permissão para provisionar contêiners Docker:
  -		
	```sh
	sudo chmod -R 777 ./mnt/airflow/logs
  -    
	```sh
	sudo chmod -R 777 ./mnt/airflow/dags
  -  
	```sh
	sudo chmod -R 777 ./mnt/airflow/plugins
  -	
	```sh
	sudo chmod -R 777 /var/run/docker.sock

Após os passos anteriores é necessário a criação das imagens utilizadas para a execução do projeto.

  - Utilizando-se do arquivo Makefile, podemos executar os comandos a seguir para instânciar toda nossa infraestrutura e executar nosso projeto:
    - build da imagem do Airflow:
      ```sh
      make build_airflow
    - build da imagem do Spark:
      ```sh
      make build_spark_apps

    Após finalizado o build das imagens acima é possível realizar os passos adiante.

  - Instanciar o ambiente Spark e Airflow:
    - Executar ambiente Airflow:
      ```sh
      make deploy_airflow
    - Executar ambiente Spark:
      ```sh
      make deploy_airflow

    Pode-se verficar se os ambientes foram provisionados através dos comandos:
    - Para ambiente Airflow:
      ```sh
      make watch_airflow
    - Para ambiente Spark:
      ```sh
      make watch_airflow

Após verificar que os ambientes estão em execução, você pode acessar a interface do Airflow pelo endereço "http://localhost:8080/". Nela, as DAGs podem ser executadas e os logs de suas execuções monitorados.

Para que as DAGs funcionem corretamente, é necessário criar uma "connection" no Airflow, garantindo o acesso às credenciais do bucket S3 na AWS. Para isso, siga os seguintes passos:

- 1 - Clique em "Admin" na parte superior da interface do Airflow.
- 2 - Selecione "Connections".
- 3 - No campo "Conn Id", insira "aws_access_abinbev".
- 4 - No campo "Conn Type", procure por "S3".
- 5 - Preencha os campos "Login" e "Password" com a access key e a secret key da AWS.
- 6 - Clique em "Salvar".
Com a conexão configurada, as DAGs já podem ser executadas no ambiente do Airflow.

No caso do processo executado com Spark, sua interface também pode ser acessada para visualizar os jobs Spark em andamento e os recursos alocados, através do endereço "http://localhost:8090/".

### Implementando a parte de monitoramento

Para que seja possível visualizar as metricas do sistema e dos contêiners acima, é necessário a implementação do nosso sistema de monitoramento fazendo uso do Prometheus e Grafana. Para isso é necessário seguir os passos abaixo:
	Executar o seguinte comando:
 	- 
	  ```sh
	  make deploy_monitoring

Com isso os serviços necessário para o monitoramento serão instanciados, é possível observar os status dos serviços executando:
	Executar o seguinte comando:
  	- 
	  ```sh
	  make watch_monitoring

Após instanciado, é possível então realizar a criação dos Dashboards no grafana.

### Adicionando dashboards ao Grafana

A interface do Grafana pode ser acessada no navegador, digitando o endereço http://localhost:3000. O usuário e senha padrão são admin e admin, respectivamente.

Para adicionar o dashboard do Node Exporter e do Docker, clique em "Dashboards" no lado esquerdo da tela, e depois em Import. No campo Grafana.com Dashboard digite o número 1860 e clique em Load. Em seguida, selecione o Prometheus como fonte de dados e clique em Import.

Obs: Caso não tenha disponível o Prometheus como Data Source, criar a connection e no campo URL do Prometheus passar o endereço "http://prometheus:9090", isso habilitará a conexão do Grafana com o Prometheus, assim será possível consumir as metricas e criar o Dashboard.

Para adicionar o dashboard referente ao Docker, repita o processo usando o ID 193 no campo Grafana.com Dashboard.

Após realizado os passos acima, será possível visualizar as métricas de sistema e contêiners Docker como as imagens demonstradas abaixo.

System Monitoring
![dashboard_system_monitoring](https://github.com/user-attachments/assets/81ace25b-73f3-49da-b4b4-321c2af7a276)

Docker Monitoring
![dashboard_docker_monitoring](https://github.com/user-attachments/assets/b171da22-af9b-4d2d-95d5-e3034618be42)


Finalizadas as execuções, pode-se desativar os ambientes através dos comandos abaixo:
  - Desativando ambiente Airflow:
    ```sh
    make stop_airflow
  - Desativando ambiente Spark:
    ```sh
    make stop_spark
  - Desativando ambiente de monitoramento:
    ```sh
    make stop_monitoring

Processo finalizado.

## Conclusão

Através do trabalho proposto, pode-se desenvolver um fluxo completo de ETL utilizando-se de algumas ferramentas disponíveis no mercado, ficando como observação a possibilidade de realizar melhorias nas etapas executadas, construindo um melhor desenvolvimento, execução e entrega.
Agradeço desde já pela oportunidade e sigo a disposição para quaisquer questionamentos.
Muito obrigado, AB Inbev.

Atenciosamente,

### Autor

Irineu Madazio Neto,
Engenheiro de Controle e Automação
Sênior Data Engineer
apaixonado pela área de Engenharia de Dados.

[![Linkedin Badge](https://img.shields.io/badge/-Irineu-blue?style=flat-square&logo=Linkedin&logoColor=white&link=https://www.linkedin.com/in/irineu-madazio-neto/)](https://www.linkedin.com/in/irineu-madazio-neto/) 

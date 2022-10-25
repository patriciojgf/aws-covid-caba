<p align="center"> 
    <h1 align="center">Trabajo practico final Foundations:</h1>
    
<p align="center">
        <h2 align="center">Estadisticas Covid-19 en CABA, Argentina</h2>
</p>
    
  <p align="center">
    El proyecto toma datos publicos de estadisticas de la ciudad de Buenos Aires y del ministerio de saludo de la nación, los procesa para obtener estadisticas de la pandemia de COVID-19 y genera tableros para ser visualizados en una pagina pagina web.
    <br>
    <br>
  </p>
</p>


## Table of contents
- [Table of contents](#table-of-contents)
- [Contenido](#contenido)
- [Resumen del proceso diario](#resumen-del-proceso-diario)
- [Visualizacion de datos](#visualizacion-de-datos)
- [S3 buckets](#s3-buckets)
- [Estructura de la red utilizada](#estructura-de-la-red-utilizada)
- [Metabase](#metabase)
- [Pagina Web](#pagina-web)
- [Bastion Host](#bastion-host)
- [Step Functions](#step-functions)
- [Log de ECS Fargate](#log-de-ecs-fargate)
- [Presupuesto minimo estimado](#presupuesto-minimo-estimado)
## Contenido

```text
AWS-COVID-CABA/
└── covidapp/
│   ├── casos.py
│   ├── vacunacion.py
│   ├── clean_tables.py
│   ├── id_tables.py
│   ├── init_run.py
│   ├── poblacion.py
│   ├── Dockerfile
│   ├── requirements.txt
└── infra/
|   ├── aws-covid-infra.pdf
|   ├── mystatemachine.json
│   └── img/
└── lambda/
│   ├── stream-casoscovid-to-s3.py
│   └── stream-vacunacion-to-s3.py
└── web/
│   └── index.html
├── .gitignore
└── README.md
``` 
## Resumen del proceso diario
- Se recibe un archivo token en el bucket s3 "start_process", esto activa una "State Machine" que ejecuta tres pasos secuenciales:
    - 1. Se ejecuta proceso lambda para descargar el archivo que tiene el detalle de vacunaciones y lo deposita en el bucket s3 "covidbucket-s3"
    - 2. Se ejecuta proceso lambda para descargar el archivo que tiene el detalle de casos covid y lo deposita en el bucket s3 "covidbucket-s3"
    - 3. Se activa una tarea ECS Fargate que limpia los datos, los transforma y carga la base de datos RDS postgresql.

## Visualizacion de datos
- Se creo una pagina web estatica (almacenada en S3) que muestra los datos de la pandemia de COVID-19 en forma de dashboards, estos dashboards son generados por Metabase en una instancia de AWS conectada al RDS donde previamente se cargaron los datos.
- Ejemplo:
- ![Visual](/infra/img/visual.png)

## S3 buckets
-  covidbucket-s3
   -  archivos fuente para la carga de base de datos.
-  start-covid-process
   -  se recibe un archivo token para iniciar el proceso.
-  www.covidcaba.com 
   -  se almacena la pagina web estatica.

## Estructura de la red utilizada
- VPC: 
  - IPv4 CIDR: 30.0.0.0/16

- Subnets Publicas: 
  - Utilizadas para Bastion Host:
    - covid-pub-sn-01
    - covid-pub-sn-02
  - Utilizadas para Metabase
    - covid-met-sn-01
    - covid-met-sn-02

- Subnets Privadas:
  - Subnet para ECS Fargate
    - covid-priv-sn-etl-01
    - covid-priv-sn-etl-02
  - Subnet para RDS postgresql
    - covid-priv-sn-db-01
    - covid-priv-sn-db-02

- Subnet Group para RDS
  - covidapp-rds-sn-group
    - covid-priv-sn-db-01
    - covid-priv-sn-db-02

## Metabase
 - Se ingresa desde la ip elastica asignada a la subnet privada de Metabase:
      - 54.161.34.62:3000

## Pagina Web
 - http://s3.amazonaws.com/www.covidcaba.com/index.html

## Bastion Host
 - Se ingresa via ssh con la key del bastion host y luego con psql desde el host a RDS:
   - <code> ssh -A ec2-user@BastionHost-NLB-7f23613c4c748f41.elb.us-east-1.amazonaws.com </code>
   - <code> psql -h cabacovid.cs4nf40vsq8e.us-east-1.rds.amazonaws.com --port=5432 -U covid_app --dbname=cabacovid </code>
- ![Visual](/infra/img/bastion.png)

## Step Functions
- Al arribo de un archivo token al bucket "start-covid-process", se ejecuta la State Machine "covid-state-machine" que ejecuta los pasos secuenciales:
    - 1. Se ejecuta proceso lambda para descargar el archivo que tiene el detalle de vacunaciones y lo deposita en el bucket s3 "covidbucket-s3"
    - 2. Se ejecuta proceso lambda para descargar el archivo que tiene el detalle de casos covid y lo deposita en el bucket s3 "covidbucket-s3"
    - 3. Se activa una tarea ECS Fargate que limpia los datos, los transforma y carga la base de datos RDS postgresql.
- ![Visual](/infra/img/steps.png)

## Log de ECS Fargate
- ![logs](/infra/img/logs.png)


## Presupuesto minimo estimado
- ![presupuesto](/infra/img/presupuesto.png)

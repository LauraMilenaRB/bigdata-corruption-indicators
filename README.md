# Implementación de un sistema *Big Data* que permita alertar en tiempo real posibles irregularidades en la contratación del gasto público


## Arquitectura propuesta en AWS

![img.png](img.png)



Para poder desplegar la arquitectura propuesta se desarrollaron los componentes requeridos con la siguiente estructura:

![img.png](img_1.png)
![img_1.png](img_2.png)

1. **artefacts:** Se encuentran las funciones requeridas para poder crear, configurar y desplegar los servicios requeridos.

2. **src:** Los archivos requeridos para configurar variables generales de la arquitectura.

3. **main.py:** Main principal para la creación de la arquitectura propuesta.

4. **main_deleted.py:** Main principal para borrar los recursos y servicios de la arquitectura propuesta.



## Manual de uso

### Para Batch

1. Instale AWS CLI en su máquina, para configurar las credenciales de uso de su cuenta principal, manual de instalación https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html

2. Compruebe la instalación AWS CLI.
   ```
    aws --version
   ```

3. Diríjase a su cuenta principal de AWS, luego al servicio IAM y cree uno nuevo de la siguiente manera:

   1. Escriba un nombre para el usuario nuevo.

   2. Luego seleccione la opción de Adjuntar políticas directamente y seleccione ***'AdministratorAccess'***

   3. Por último, revise la configuración y seleccione ***'Crear Usuario'***

   4. Cuando lo haya creado seleccione el usuario y diríjase a ***'Credenciales de seguridad'***

   5. Busque ***'Claves de acceso'*** y seleccione ***'Crear clave de acceso'***, luego de esto siga los pasos y descargue las credenciales.

   6. Luego abra la terminal de su computador y escriba:
      ```
      aws configure
      ```
   
   7. Teniendo a mano el archivo de credenciales, complete lo siguiente:
      ```
      AWS Access Key ID:
      AWS Secret Access Key:
      Default region name:
      ```
      
   8. Verifique las credenciales configuradas
      ```
      aws configure list
      ```

4. Descargue y clone el repositorio del código fuente
   ```
   git clone https://github.com/LauraMilenaRB/ref-arch-corruption-indicators.git
   ```

5. Configure las variables requeridas para la creación de los buckets, el servicio de apache airflow y las vpc

   1. **Variables S3 Buckets**

      ![img_5.png](img_5.png)

      1. **bucket_names:** Lista de los nombres de los buckets a crear.

      2. **bucket_dag_name:** Nombre del bucket que contiene el DAG del servicio Amazon MWAA.

      3. **prefix:** El prefix que se agregara a los nombres de los buckets.

      4. **path_src_local_files:** Ruta local donde se encuentra la estructura de los buckets y archivos a subir.

         Esta estructura tiene el mismo nombre de los cubos a crear para así poder identificar los archivos que se deben subir a cada uno.

        ![img_6.png](img_6.png)

         * **req-files/dags:** Contiene el código del flujo de datos del DAG. Un DAG es una colección de tareas organizadas que se programan y ejecutan de acuerdo a las necesidades del usuario.

         * **req-files/scripts:** Contiene los archivo del código fuente de los ETL ***(etl)*** y procesamientos de indicadores de corrupcion ***(ind)***

   2. **Variables VPCs**

      ![img_22.png](img_22.png)

      1. **vpc_name:** Nombre de la VPC a crear. 

      2. **path_template_vpc_cloudformation:** Ruta local donde se encuentra el archivo template YAML de cloud formation para crear la VPC.

      3. **capabilities:** En algunos casos, debe reconocer explícitamente que su plantilla de pila contiene ciertas capacidades para que CloudFormation cree la pila.

      4. **vpcCIDR:** El rango de IP para la VPC

      5. **publicsCIDR:** Las IPs de las subnets públicas de la VPC, para esta VPC se tienen 3 subnets públicas.

      6. **privatesCIDR:** Las IPs de las subnets privadas de la VPC, para esta VPC se tienen 3 subnets privadas.

   3. **Variables Amazon MWAA (Apache Airflow)**
      ![img_23.png](img_23.png)
      1. **evn_mwaa_name:** Nombre del ambiente MWAA a crear. 

   4. **Variables Amazon Kinesis**
   ![img_24.png](img_24.png)
   5. **Variables stream Amazon EMR**
   ![img_25.png](img_25.png)
   6. **Variables Redshift**
   ![img_26.png](img_26.png)

6. Ejecute el Main principal, este tiene las funciones de creación de S3 Buckets, Apache airflow, VPCs, Kinesis, Amazon EMR y Amazon Redshift.
   ![img_4.png](img_4.png)

7. Verifique la creación de los buckets, carpetas y carga de archivos correspondientes en la consola de servicios de Amazon S3, este proceso puede demorar un poco si se suben archivos locales.
   ![img_7.png](img_7.png)
   ![img_8.png](img_8.png)
8. Verifique la creación del cluster de Amazon Redshift, para esto diríjase en la consola de servicios de Amazon Redshift. y debe ver el cluster disponible,
   ![img_44.png](img_44.png)
   1. De clic en el cluster, luego en *'Datos de consulta'* > *Consulta en el editor de consultas v2*
   ![img_45.png](img_45.png)
   2. Verifiqué que las tablas de resultados que haya definido en el conf se hayan creado correctamente.
   ![img_46.png](img_46.png)
   
9. Verifique la creación de la VPCs. Diríjase a CloudFormation en la consola de servicios de Amazon.

   ![img_9.png](img_9.png)

10. Verifique la creación de Amazon MWAAA (Apache Airflow).

    1. Primero, verifique la creación de roles y políticas asociadas al servicio.
    ![img_10.png](img_10.png)
    ![img_27.png](img_27.png)
    2. Luego verifique que el entorno de Airflow este en creación. Tenga en cuenta que la creación de este servicio puede demorar entre 30 minutos a 1 hora aproximadamente.
    ![img_11.png](img_11.png)

    3. **IMPORTANTE:** Debe configurar algunas variables requeridas antes de abrir la interfaz de usuario de Airflow.

       1. Para configurar el DAG, edité los siguientes archivos:

          ![img_12.png](img_12.png)

          * **DAG-ContractingIndicators.py:** Contiene el código Python con el paso a paso de las tareas y servicios organizados para ejecutar.

          * **vars_emr_jobs.py:** Contiene las variables requeridas para el DAG.

            1. **endpoint_url_arg:** Diccionario de las URLs para descargar los archivos del nuestro caso de uso, Contratación Pública.
            ![img_16.png](img_16.png)
            2. **ind_sources:** Contiene los datos fuentes requeridos para poder empezar a procesar cada uno de los indicadores de corrupción.
            ![img_17.png](img_17.png)

            3. **JOB_FLOW_OVERRIDES:** Contiene toda la configuración para desplegar los clústeres de Spark para la ejecución de los ETLs y procesamientos.

               * **LogUri:** Modifique esta variable con el nombre del bucket que guardara los Logs del clúster

               * **Ec2SubnetId:** Apenas despliegue la VPC verifique las subnets privadas desplegadas y remplace el valor existente por el Id de la subnet que tiene como descripción ***'vpc-mwaa - EMR - Private Subnet (AZ3)'***.

                 **NOTA IMPORTANTE**: Esta variable debe configurarse correctamente para que el servicio Amazon EMR se despliegue correctamente.

                 ![img_28.png](img_28.png)

                 ![img_20.png](img_20.png)

               * **StepConcurrencyLevel:** Esta variable determina cuantos steps concurrentes se pueden ejecutar en los clúster de EMR.

               * En este apartado hay muchas más variables que puede modificar, pero en este caso la configuración presentada es lo mínimo requerido para aprovisionar el servicio de EMR. Mas detalle https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/emr/client/run_job_flow.html

               ![img_18.png](img_18.png)

            4. * **endpoint_conn_arg:** .
               * **bd_name_arg:** El nombre de la base de datos de Amazon Redshift para crear la tabla a consultar por quicksight.
               * **user_db_arg:** El usuario de la base de datos de Amazon Redshift.
               * **psw_db_arg:** La contraseña de la base de datos de Amazon Redshift.
               * **deleted_data_results:** La consulta para borrar los datos cargados en Amazon Redshift.
               * **insert_data_results:** La consulta para insertar datos en Amazon Redshift. 

               ![img_19.png](img_19.png)

       2. Luego de editar las variables requeridas diríjase al Bucket S3 donde se almacena el archivo DAG.py y renplacelo por la nueva configuración.

       ![img_29.png](img_29.png)

    4. Cuando se habilite el entorno y modifique las variables, puede dar clic en *'Abrir la interfaz de usuario de Airflow'*. 

       * Si el DAG contiene algún error, verá algo como esto: ![img_13.png](img_13.png)

       * Si el DAG no contiene ningún error, verá algo como esto: ![img_14.png](img_14.png)

    5. Al dar clic en el DAG puede observar el detalle del flujo de datos. ![img_30.png](img_30.png)

    6. Para iniciar el flujo debe dar clic en el botón *'Play'* solo si *'Next Run'* está configurado con una fecha futura, en dado caso que esté configurado con una fecha anterior a la actual solo debe habilitar el interruptor *'Pause/Unpause DAG'* que aparece en la esquina superior izquierda.

       ![img_36.png](img_36.png)

    7. Verifique el estado del flujo en la opción de *'Graph'*. Verá algo como esto:

       ![img_15.png](img_15.png)

    8. Cuando el DAG se inicia el flujo de datos cambia de color de acuerdo a las convenciones definidas.

       ![img_21.png](img_21.png)

       1. Si en el flujo se presenta alguna falla, puede verificar el log dando clic sobre la tarea *'failed'* y luego dar clic en *'Log'*
         ![img_31.png](img_31.png)
      
       ![img_32.png](img_32.png)

       2. También puede verificar los procesos batch en ejecución de spark buscando en la consola de servicios Amazon EMR. 

       ![img_34.png](img_34.png) ![img_35.png](img_35.png)

       Aquí debe verificar que todos los steps queden en estado *'Completado'*, en caso de que alguno falle puede validar el log correspondiente

    9. Una de las ventajas de Airflow es que podemos personalizar la programación de los DAG en un horario definido y para nuestro caso de uso programamos el DAG a ejecutarse una vez por semana, cada domingo. 
      A continuación puede ver en calendario configurado:
      ![img_33.png](img_33.png)

    10. Espere a que se complete todo el flujo de datos y verifique la creación de la tabla de resultados en Redshift.
      ![img_37.png](img_37.png)


### Para Streaming

Luego de verificar el despliegue de la arquitectura batch puede verificar el despliegue de la arquitectura para el flujo de datos en tiempo real.
1. Vaya a la consola de servicios y busqué kinesis. 

   1. Luego verifique que se haya creado kinesis data stream.
      ![img_41.png](img_41.png)
   2. Verifique que se haya creado kinesis firehose.
      ![img_42.png](img_42.png)

2. Verifique que se haya creado correctamente el clúster del servicio Amazon EMR y que se estén ejecutando los steps *'stream_etl'* y *'stream_ind'*. 
    ![img_47.png](img_47.png)
   **Nota:** En este caso solo hay dos steps los cuales se ejecutaran indefinidamente, ya que deben estar disponibles lo mismo que dure el consumidor enviando contratos para ser procesados.

3. Para hacer uso de este flujo es necesario tener un productor el cual se encargara de enviar los datos a procesar, para nuestro caso de uso se desarrolló el consumidor *'producer_contracts'*.
   Este se encarga de generar contratos de manera automática y enviarlos a kinesis data stream cada 15 segundos aproximadamente. Ejecútelo cuando haya verificado que los steps se estén ejecutando.
   ![img_43.png](img_43.png)

### Visualización y representación de datos

1. Para validar los resultados y graficarlos vamos a la consola de Amazon QuickSight. 

    1. Seleccione y de clic en *'Conjunto de datos'* *'Nuevo conjunto de datos'*.
       ![img_38.png](img_38.png)
    2. Luego busque la opción *'Redshift' (Detección automatica)*, agregue los datos solicitados y haga clic en *'Crear origen de datos'*.
       ![img_37.png](img_37.png)
    3. Ya con este paso realizado podemos realizar la creación de las gráficas requeridas para visualizar y entender de una manera grafica el comportamiento de los datos.
       ![img_39.png](img_39.png)
    4. Aquí puede configurar y usar otras herramientas de BI como Power BI, Tableau o Microstrategy utilizando los valores típicos de conexión a una base de datos. El host, nombre de la base de datos, usuario y contraseña.
       ![img_40.png](img_40.png)
    

    


    
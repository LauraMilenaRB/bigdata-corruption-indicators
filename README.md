# Implementación de un sistema Big Data que permita alertar en tiempo real posibles irregularidades en la contratación del gasto público

## Arquitectura propuesta en AWS
![img.png](img.png)

Para poder desplegar la arquiectura propuesta se desarrollaron los componentes requeridos con la siguiente estructura:

![img.png](img_1.png)

![img_1.png](img_2.png)

1. **artefacts:** Se ecuentran las funciones requeridas para poder crear, configurar y desplegar los servicios requeridos.
2. **src:** Los archivos requeridos para configurar variables generales de la arquiectura.
3. **main.py:** Main principal para la creacion de la arquietcura propuesta.
4. **main_deleted.py:** Main principal para borrar los recursos y servicios de la arquietcura propuesta.

### Manual de uso
1. Instale AWS CLI en su maquina, para configurar las credenciales de uso de su cuenta principal, manual de instalacion https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html
2. Compruebe la instalacion AWS CLI.
   ```
    aws --version
   ```
3. Dirijase a su cuenta principal de AWS, luego al servicio IAM y cree uno nuevo de la siguiente manera:
   1. Escriba un nombre para el usuario nuevo.
   2. Luego seleccione la opcion de Adjuntar politicas directamente y seleccione ***'AdministratorAccess'***
   3. Por ultimo revise la configuracion y sleccione ***'Crear Usuario'***
   4. Cuando lo haya creado seleccione el usuario y dirijase a ***'Credenciales de seguridad'***
   5. Busque ***'Claves de acceso'*** y seleccione ***'Crear clave de acceso'***, luego de esto siga los pasos y descargue las credenciales.
   6. Luego abra la terminal de su computador y escriba los siguiente:
   ```
    aws configure
   ```
   7. Teniendo a mano el archivo de credenciales, complete lo siguiente:
   ```
    AWS Access Key ID: <Escriba el access_key_id del archivo de credenciales>
    AWS Secret Access Key : <Escriba el secret_access_key del archivo de credenciales>
    Default region name: <Escriba la region en la que quiere desplegar sus servicios Ejm: us-east-1> 
   ```
   8. Verifique las credenciales configuradas
   ```
    aws configure list
   ```
4. Descargue y clone el respositorio del codigo fuente
    ```
    git clone https://github.com/LauraMilenaRB/ref-arch-corruption-indicators.git
   ```
5. Configure las variables requeridas para la creacion de los buckets, el servicio de apache airflow y las vpc
   ![img_5.png](img_5.png)
   1. **Variables Buckets**
      1. **bucket_names:** Lista de los nombres de los buckets a crear.
      2. **bucket_dag_name:** Nombre del bucket que contiene el DAG del servicio Amazon MWAA.
      3. **prefix:** El prefix que se agregara a los nombre de los buckets.
      4. **path_src_local_files:** Ruta local donde se ecuentra la estructura de los buckets y archivos a subir.
         Esta estructura tiene el mismo nombre de los cubos a crear para pode rmapear los archivos que se deben subir a cada uno.
         ![img_6.png](img_6.png)
         * **req-files/dags:** Contiene el codigo del flujo de datos del DAG.
         * **req-files/scripts:** Contiene los archivo del codigo fuente de los ETL ***(etl)*** y procesamientos de indicadores de corrupcion ***(ind)***
   2. **Variables VPCs**
      1. **vpc_name:** Nombre de la VPC a crear. 
      2. **path_template_vpc_cloudformation:** Ruta local donde se ecuentra el archivo template YAML de cloud formation para crear la VPC.
      3. **capabilities:** En algunos casos, debe reconocer explícitamente que su plantilla de pila contiene ciertas capacidades para que CloudFormation cree la pila.
      4. **vpcCIDR:** El rango de IP para la VPC
      5. **publicsCIDR:** Las IPs de las subnets publicas de la VPC, para esta VPC se tienen 3 subnets publicas.
      6. **privatesCIDR:** Las IPs de las subnets privadas de la VPC, para esta VPC se tienen 3 subnets privadas.
   3. **Variables airflow**
      1. **evn_mwaa_name:** Nombre del ambiente MWAA a crear. 
      
6. Ejecute el Main principal, este tiene las funciones de creacion de buckets, apache airflow y vpcs.
    ![img_4.png](img_4.png)
7. Verifier la creaacion de los buckets, carpetas y carga de archivos correspondientes.

   ![img_7.png](img_7.png)![img_8.png](img_8.png)![img_9.png](img_9.png)
8. Verifique la creacion de la VPCs. Dirijase a CloudFromation
   ![img_10.png](img_10.png)
9. Verifique la creacion del MWAAA. Tenga en cuenta que la creacion de este servicio se demora entre 30 a 40 minutos aproximadamente.
   ![img_11.png](img_11.png)
   1. Cuando haya creado el entorno MWAAA, puede abrir la interfaz de usuario de airflow. 
      * Si el DAG tiene algun error al abrir la interfaz de usuario, vera algo como esto: ![img_13.png](img_13.png) 
      * Si no hay ningun error en el DAG, vera algo como esto: ![img_14.png](img_14.png)
   2. Al abrir el DAG puede observar el workflow de datos y para iniciarlo debe dar click en el boton de play.
      ![img_15.png](img_15.png)
      1. Para configurar el DAG edite los siguientes archivos:
         Estos archivos al ser modificados deben ser cargados nuevamente en el bucket **req-files/dags/** para su actualizacion.
         ![img_12.png](img_12.png)
         * **DAG-ContractingIndicators.py:** Contiene la logica de las dependencias de los servicios requeridos 
         * **vars_emr_jobs.py:** Contiene las variables requeridas para el DAG.
           1. **endpoint_url_arg:** Diccionario de las URLs para descargar los arcchivos 
           ![img_16.png](img_16.png)
           2. **ind_sources:** Contiene los datos fuentes requeridos para poder empezar a procesar cada uno de los indicadores de corrupcion
           ![img_17.png](img_17.png)
           3. **JOB_FLOW_OVERRIDES:** Contiene toda la configuracion para desplegar los clusters de Spark para la ejecucion de los ETLs y procesaientos.
              * **LogUri:** Modifique esta variable con el nombre del bucket que guardara los Logs del cluster
              * **Ec2SubnetId:** Apenas despliegue la VPC verifique las subnets privadas desplegadas y remplace el valor existente por el Id de la subnet que tiene como descripcion ***vpc-mwaa - EMR - Private Subnet (AZ3)***
              ![img_20.png](img_20.png)
              * **StepConcurrencyLevel:** Esta variable determina cuantos steps concurrentes se pueden ejecutar en los cluster de EMR.
              * En este apartado hay muchas mas variables que puede modificar pero en este caso la configuracion presentada es lo minimo requerido para aprovisionar el servicio de EMR. Mas detalle https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/emr/client/run_job_flow.html
           ![img_18.png](img_18.png)
           4. * **athena_database:** El nombre de la base de datos de Athena para crear la tabla a consultar por quicksight.
              * **output_location_athena:** El nombre del bucket donde se guardaran los resultados del query ejecutado. 
              * **DDL_results:** El DDL de la tabla final que contiene los resultados de los procesmaientos de indicadores de corrupcion. 
              ![img_19.png](img_19.png)
      2. Si realizo una modificacion y subio nuevamente los archivos DAG-ContractingIndicators y vars_emr_jobs, regrese a la interfaz de usuario y elimine el DAG. 
      Luego oprima F5 y espere a que aparezca nuevamente y ejecutelo con el boton play.
      3. Cuando el DAG se incia el workflow de datos cambia de color deacuerdo a las convenciones definidas.
      ![img_21.png](img_21.png)
      
      
   
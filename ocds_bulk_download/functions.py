import psycopg2, datetime, sys, os, csv, json, codecs, hashlib, shutil, uuid, copy
import dateutil.parser, dateutil.tz
import flattentool
from zipfile import ZipFile, ZIP_DEFLATED

path = os.path.abspath(os.path.join(os.path.dirname(__file__),"..", "portalocdspy"))
sys.path.append(path)

import settings

carpetaArchivos = "archivos_estaticos/"

dbAdminConnection = settings.DATABASES["portaledcahn_admin"]
dbKingfisherConnection = settings.DATABASES["bdkingfisher"]

"""
    Funcion que obtiene la ruta raiz del proyecto y anexa al directorio. 
    Parametros
        directorio: direccion de una carpeta o archivo dentro del proyecto. 
"""
def getRootPath(directorio):
    raiz = os.path.dirname(os.path.realpath(__file__))
    archivoSalida = os.path.join(raiz, directorio)
    return archivoSalida

"""
    Funcion que se conecta a PostgreSQL (Kingfisher)
    Extrae los releases en un archivo .csv
"""
def obtenerRelasesCSV():
    con = None
    nombreArchivo = "releases.csv"
    carpetaArchivos = "archivos_estaticos"

    select = """
        SELECT
            r.release_id,
            r.ocid,
            d.hash_md5,
            r.package_data_id,
            d."data" as "release",
            pd."data" as "package" 
        FROM release r
            INNER JOIN data d on r.data_id = d.id 
            INNER JOIN package_data pd on r.package_data_id = pd.id
        GROUP BY 
            r.release_id, 
            r.ocid, 
            d.hash_md5, 
            r.package_data_id, 
            d.data, 
            pd.data
        ORDER BY
            r.release_id
        --LIMIT 2
    """

    try:
        raiz = os.path.dirname(os.path.realpath(__file__))
        archivoSalida = os.path.join(raiz, carpetaArchivos, nombreArchivo)
        query = "copy ({0}) To STDOUT With CSV DELIMITER '|';".format(select)

        con = psycopg2.connect(
            host=dbKingfisherConnection["HOST"], 
            database=dbKingfisherConnection["NAME"], 
            user=dbKingfisherConnection["USER"], 
            password=dbKingfisherConnection["PASSWORD"],
            port=dbKingfisherConnection["PORT"]
        )

        cur = con.cursor()

        with open(archivoSalida, 'w') as f_output:
            cur.copy_expert(query, f_output)

        f_output.close()

    except psycopg2.DatabaseError as e:
        print(f'Error {e}')
        sys.exit(1)

    except IOError as e:
        print(f'Error {e}')
        sys.exit(1)

    finally:
        if con:
            con.close()

"""
    Guarda el archivo metada de la descarga masiva de archivos en postgres. 
"""
def guardarDataJSON(json):
    conn = None

    id = str(uuid.uuid1())
    createdby = "Portal de Contrataciones Abiertas de Honduras"
    active = True

    query = '''
        INSERT INTO descargas(id, file, createdby, active)
        VALUES (%s, %s, %s, %s)
    '''

    values = (id, json, createdby, active)

    try:
        conn = psycopg2.connect(
            host=dbAdminConnection["HOST"], 
            database=dbAdminConnection["NAME"],
            user=dbAdminConnection["USER"], 
            password=dbAdminConnection["PASSWORD"],
            port=dbAdminConnection["PORT"]
        )

        cur = conn.cursor()
        cur.execute(query, values)
        conn.commit()
        cur.close()
        
    except (Exception, psycopg2.DatabaseError) as error:
        print('Error en el insert')
        print(error)
    finally:
        if conn is not None:
            conn.close()

def md5(fname):
    fname = getRootPath(fname)

    hash_md5 = hashlib.md5()

    with open(fname, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)

    return hash_md5.hexdigest()

def limpiarArchivos(directorio):
    directorio = getRootPath(directorio)
    listaArchivos = [ f for f in os.listdir(directorio) if f.endswith(".txt") ]

    for a in listaArchivos:
        open(directorio + a, 'w').close()

def crearDirectorio(directorio):
    directorio = getRootPath(directorio)

    try:
        os.stat(directorio)
    except:
        os.mkdir(directorio)

def escribirArchivo(directorio, nombre, texto, modo='a'):
    direccionArchivo = getRootPath(directorio + nombre)
    archivoSalida = codecs.open(direccionArchivo, modo, 'utf-8')
    archivoSalida.write(texto)
    archivoSalida.write('\n')
    archivoSalida.close()

def aplanarArchivo(ubicacionArchivo, directorio):
    directorio = getRootPath(directorio)

    ubicacionArchivo = getRootPath(ubicacionArchivo)

    flattentool.flatten(
        ubicacionArchivo,
        output_name=directorio,
        main_sheet_name='releases',
        root_list_path='releases',
        root_id='ocid',
        # schema=carpetaArchivos + 'release-schema.json',
        disable_local_refs=True,
        remove_empty_schema_columns=True,
        root_is_list=False
    )

    with ZipFile(directorio + '.zip', 'w', compression=ZIP_DEFLATED) as zipfile:
        for filename in os.listdir(directorio):
            zipfile.write(os.path.join(directorio, filename), filename)
    shutil.rmtree(directorio)

    # print('flatten ok')

def generarMetaDatosPaquete(paquetes, md5):

    uri = ''
    license = ''
    version = '1.1'
    publisher = {}
    extensions = []
    publishedDate = ''
    publicationPolicy = ''
    releases = []

    metaDatosPaquete = {}

    fechaActual = datetime.datetime.now(dateutil.tz.tzoffset('UTC', -6*60*60))
    publishedDate = fechaActual.isoformat()

    for p in paquetes:

        paquete = json.loads(p)

        license = paquete['license']
        version = paquete['version']
        publisher = paquete['publisher']
        publicationPolicy = paquete['publicationPolicy']

        for e in paquete['extensions']:
            if not e in extensions:
                extensions.append(e)

    metaDatosPaquete["uri"] = 'http://contratacionesabiertas.gob.hn/descargas/' + md5 + '.json'
    metaDatosPaquete["version"] = version
    metaDatosPaquete["publishedDate"] = publishedDate
    metaDatosPaquete["publisher"] = publisher
    metaDatosPaquete["extensions"] = extensions
    metaDatosPaquete["license"] = license
    metaDatosPaquete["publicationPolicy"] = publicationPolicy

    return metaDatosPaquete

def generarReleasePackage(paquete, releases, directorio, nombre):
    contador1 = 0
    contador2 = 0
    archivoJson = getRootPath(directorio + nombre)

    f = codecs.open(archivoJson, "w", "utf-8")

    #Cargando la data del paquete
    paquete = getRootPath(paquete)

    metaDataPaquete = codecs.open(paquete, "r", "utf-8")
    metaData = metaDataPaquete.readlines()
    metaDataPaquete.close()

    for l in metaData[:-1]:
        f.write(l)

    #Creando una estructura para el listado de releases.
    f.write(',"releases": [\n')

    #cargando la data de releases 
    releases = getRootPath(releases)

    with open(releases, encoding="utf-8") as infile:
        for linea in infile:
            contador1 += 1 

    # Quitando la ultima coma ,
    with open(releases, encoding="utf-8") as infile:
        for linea in infile:
            if contador2 == contador1 - 1:
                f.write(linea[:-2])
            else:
                f.write(linea)

            contador2 += 1

    #Cerrando el archivo json
    f.write('\n]\n}')

    f.close()

def generarArchivosEstaticos(file):
    contador = 0
    archivos = {}
    archivosProcesar = []
    directorioReleases = carpetaArchivos + 'releases/' 
    directorioHashReleases = directorioReleases + 'hash/'
    directorioTxtReleases = directorioReleases + 'txt/'
    directorioPaquetes = directorioReleases + 'paquetes/'
    directorioDescargas = directorioReleases + 'descargas/'

    numeroColumnaReleaseId = 0
    numeroColumnaOCID = 1
    numeroColumnaHASH = 2
    numeroColumnaPaqueteId = 3
    numeroColumnaRelease = 4
    numeroColumnaPaquete = 5

    crearDirectorio(directorioReleases)
    crearDirectorio(directorioHashReleases)
    crearDirectorio(directorioTxtReleases)
    crearDirectorio(directorioPaquetes)
    crearDirectorio(directorioDescargas)

    limpiarArchivos(directorioHashReleases)
    limpiarArchivos(directorioTxtReleases)
    limpiarArchivos(directorioPaquetes)

    csv.field_size_limit(sys.maxsize)
    with open(file) as fp:

        reader = csv.reader(fp, delimiter='|')

        #Recorriendo el archivos csv.
        for row in reader:
            llave = ''
            source = ''
            publisher = ''

            contador += 1

            dataRelease = json.loads(row[numeroColumnaRelease])
            dataPaquete = json.loads(row[numeroColumnaPaquete])

            year = dataRelease["date"][0:4]

            month = dataRelease["date"][5:7]

            if 'name' in dataPaquete["publisher"]:
                llave = llave + dataPaquete["publisher"]["name"].replace('/', '').replace(' ', '_')[0:17].lower()
                publisher = dataPaquete["publisher"]["name"]

            if 'sources' in dataRelease:
                if 'id' in dataRelease["sources"][0]:
                    llave = llave + '_' + dataRelease["sources"][0]["id"]

                if 'name' in dataRelease["sources"][0]:
                    source = dataRelease["sources"][0]["name"]

            llave = llave + '_' + year + '_'+ month

            if not llave in archivos:
                archivos[llave] = {}
                archivos[llave]["year"] = year
                archivos[llave]["month"] = month
                archivos[llave]["sistema"] = source
                archivos[llave]["publicador"] = publisher
                archivos[llave]["paquetesId"] = []
                archivos[llave]["paquetesData"] = []
                archivos[llave]["archivo_hash"] = directorioHashReleases + llave + '_hash.txt'
                archivos[llave]["archivo_text"] = directorioTxtReleases + llave + '_releases.txt'
                archivos[llave]["archivo_paquete"] = directorioPaquetes + llave + '_paquete.json'

            if not row[numeroColumnaPaqueteId] in archivos[llave]["paquetesId"]:
                archivos[llave]["paquetesId"].append(row[numeroColumnaPaqueteId])
                archivos[llave]["paquetesData"].append(row[numeroColumnaPaquete])

            escribirArchivo(directorioHashReleases, llave + '_hash.txt', row[numeroColumnaHASH])
            escribirArchivo(directorioTxtReleases, llave + '_releases.txt', row[numeroColumnaRelease] + ',')

        print('Cantidad de releases ->', contador)

        #Cargar el ultimo metadatos_releases.
        ultimoArchivo = directorioReleases + 'metadata_releases.json'

        try:
            with open(getRootPath(ultimoArchivo), encoding="utf-8") as json_file:
                archivosProcesados = json.load(json_file)
        except Exception as e:
            print(e)
            archivosProcesados = {}

        #Comparar archivos MD5
        for llave in archivos:
            archivo = archivos[llave]
            archivo["urls"] = {}
            archivo["finalizo"] = False
            archivo["md5_hash"] = md5(archivo["archivo_hash"])

            # Preguntar si el archivo ya ha sido procesado
            if llave in archivosProcesados:
                archivo["finalizo"] = archivosProcesados[llave]["finalizo"]

                # Si los hash son diferentes entonces se procesa.
                if archivo["md5_hash"] != archivosProcesados[llave]["md5_hash"]:
                    archivosProcesar.append(llave)
                else:
                    # Si no se termino de procesar completo, entonces se procesa de nuevo.
                    if archivosProcesados[llave]['finalizo'] == False:
                        aniosPorProcesar.append(llave)
                    else: 
                        archivos[llave] = copy.deepcopy(archivosProcesados[llave])
            else:
                # Si el archivo nunca habia sido procesado, entonces se procesa. 
                archivosProcesar.append(llave)
                archivosProcesados[llave] = archivos[llave]

        print("Archivos por procesar: ", archivosProcesar)

        #Generar release package
        for llave in archivos:
            if llave in archivosProcesar:
                #Generando metadatos del paquete                
                metaDataPaquete = generarMetaDatosPaquete(archivos[llave]['paquetesData'], archivos[llave]['md5_hash'])
                escribirArchivo(directorioPaquetes, llave + '_paquete.json', json.dumps(metaDataPaquete, indent=4, ensure_ascii=False), 'w')
                
                #Generando Json 
                generarReleasePackage(archivos[llave]["archivo_paquete"], archivos[llave]["archivo_text"], directorioDescargas, llave + '.json')
               
                archivos[llave]["urls"]["json"] = llave + '.json'
                archivos[llave]["urls"]["md5"]  = llave + '.md5'
                archivos[llave]["urls"]["xlsx"] = llave + '.xlsx'
                archivos[llave]["urls"]["csv"]  = llave + '.zip'
                
                md5_json = md5(directorioDescargas + archivos[llave]["urls"]["json"])
                archivos[llave]["md5_json"] = md5_json

                #Generando MD5
                escribirArchivo(directorioDescargas, llave + '.md5', md5_json, 'w')

                # Eliminando variables no necesarias para almacenar
                del archivos[llave]["paquetesData"]
                del archivos[llave]["paquetesId"]
                del archivos[llave]["archivo_hash"]
                del archivos[llave]["archivo_text"]
                del archivos[llave]["archivo_paquete"]

                archivosProcesados[llave] = archivos[llave]

        #Aplanando archivos .json
        for llave in archivos:
            if llave in archivosProcesar:
                #Generando CSV, EXCEL
                aplanarArchivo(directorioDescargas + archivos[llave]["urls"]["json"], directorioDescargas + llave)
                archivos[llave]["finalizo"] = True
                archivosProcesados[llave]["finalizo"] = True

        escribirArchivo(directorioReleases, 'metadata_releases.json', json.dumps(archivosProcesados, ensure_ascii=False), 'w')
        guardarDataJSON(json.dumps(archivosProcesados, ensure_ascii=False))

def pruebas():
    data = {}
    metadatos = getRootPath('archivos_estaticos/releases/' + 'metadata_releases.json')

    print(metadatos)

    with open(metadatos) as json_file:
        data = json.load(json_file)

    guardarDataJSON(json.dumps(data, indent=4, ensure_ascii=False))

def main():
    archivoReleases = "releases.csv"
    path = getRootPath(carpetaArchivos)
    file = path + archivoReleases

    startDate = datetime.datetime.now()
    print(datetime.datetime.now())

    obtenerRelasesCSV()
    generarArchivosEstaticos(file)
    # pruebas()

    endDate = datetime.datetime.now()
    print(datetime.datetime.now())

    minutes = ((endDate-startDate).seconds) / 60

    print("Tiempo transcurrido: " + str(minutes) + " minutos")

main()
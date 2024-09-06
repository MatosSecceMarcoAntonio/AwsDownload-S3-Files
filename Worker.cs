using Amazon.Runtime;
using Amazon.S3.Model;
using Amazon.S3;
using System.IO;

namespace AwsDownloadFIle
{
    public class Worker : BackgroundService
    {
        private readonly ILogger<Worker> _logger;
        private readonly IConfiguration _configuration;
        private IAmazonS3 _s3Client;
        private string _bucketName;

        public Worker(ILogger<Worker> logger, IConfiguration configuration)
        {
            _logger = logger;
            _configuration = configuration;

            // Configurar el cliente S3 utilizando la configuración del appsettings.json
            var awsOptions = _configuration.GetSection("AWS");
            var accessKey = awsOptions["AccessKey"];
            var secretKey = awsOptions["SecretKey"];
            var region = awsOptions["Region"];
            _bucketName = awsOptions["BucketName"];

            // Inicializar el cliente de S3 usando AWS directamente (sin ServiceURL)
            _s3Client = new AmazonS3Client(
                new BasicAWSCredentials(accessKey, secretKey),
                Amazon.RegionEndpoint.GetBySystemName(region) // Configurar la región, por ejemplo, us-east-1
            );
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _logger.LogInformation("Worker iniciado en: {time}", DateTimeOffset.Now);

            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    string continuationToken = null;

                    do
                    {
                        // Llamada para listar archivos del bucket con paginación
                        var request = new ListObjectsV2Request
                        {
                            BucketName = _bucketName,
                            ContinuationToken = continuationToken // Token para obtener el siguiente lote de archivos
                        };

                        var listResponse = await _s3Client.ListObjectsV2Async(request, stoppingToken);

                        foreach (var s3Object in listResponse.S3Objects)
                        {
                            // Ruta local base
                            string destinationPath = @"D://Fotos";

                            // Crear la ruta completa del archivo, incluidas subcarpetas
                            string fullPath = Path.Combine(destinationPath, s3Object.Key);

                            // Verificar si el objeto es una carpeta (termina con "/")
                            if (s3Object.Key.EndsWith("/"))
                            {
                                // Crear la carpeta local si no existe
                                if (!Directory.Exists(fullPath))
                                {
                                    Directory.CreateDirectory(fullPath);
                                    _logger.LogInformation($"Carpeta creada: {fullPath}");
                                }
                                continue; // Omitir la descarga de carpetas
                            }

                            // Verificar si el archivo ya existe y comparar tamaños
                            if (File.Exists(fullPath))
                            {
                                var fileInfo = new FileInfo(fullPath);

                                // Si el tamaño es el mismo, omitimos la descarga
                                if (fileInfo.Length == s3Object.Size)
                                {
                                    _logger.LogInformation($"Archivo ya existe y coincide el tamaño: {s3Object.Key}");
                                    continue; // Saltar este archivo
                                }
                            }

                            _logger.LogInformation($"Descargando archivo: {s3Object.Key}");

                            // Descargar el archivo
                            var getRequest = new GetObjectRequest
                            {
                                BucketName = _bucketName,
                                Key = s3Object.Key
                            };

                            using (var response = await _s3Client.GetObjectAsync(getRequest, stoppingToken))
                            using (var responseStream = response.ResponseStream)
                            {
                                // Crear las subcarpetas si no existen
                                string directoryPath = Path.GetDirectoryName(fullPath);
                                if (!Directory.Exists(directoryPath))
                                {
                                    Directory.CreateDirectory(directoryPath);
                                }

                                // Guardar el archivo
                                using (var fileStream = new FileStream(fullPath, FileMode.Create, FileAccess.Write))
                                {
                                    await responseStream.CopyToAsync(fileStream, stoppingToken);
                                }

                                _logger.LogInformation($"Archivo descargado en: {fullPath}");
                            }
                        }

                        // Actualizar el token para la próxima solicitud
                        continuationToken = listResponse.IsTruncated ? listResponse.NextContinuationToken : null;

                    } while (continuationToken != null); // Continuar hasta que no haya más objetos

                }
                catch (AmazonS3Exception e)
                {
                    _logger.LogError($"Error al interactuar con S3: {e.Message}");
                }
                catch (Exception e)
                {
                    _logger.LogError($"Error general: {e.Message}");
                }

                // Espera antes de la próxima ejecución
                await Task.Delay(TimeSpan.FromHours(1), stoppingToken); // Esperar una hora entre iteraciones
            }
        }
    }
}

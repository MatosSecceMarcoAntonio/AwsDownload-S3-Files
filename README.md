# AwsDownload S3 Files

Servicio en segundo plano desarrollado en **C# (.NET 6)** que descarga automáticamente archivos desde un **bucket de Amazon S3** hacia un directorio local. El servicio se ejecuta de forma continua, revisando el bucket cada hora y descargando únicamente los archivos nuevos o modificados.

---

## 📋 Características

- 🔄 Descarga automática y periódica (cada 1 hora) de archivos desde Amazon S3.
- 📂 Preserva la estructura de carpetas del bucket en el sistema de archivos local.
- ✅ Evita descargas duplicadas comparando el tamaño del archivo local con el almacenado en S3.
- 📄 Soporte de paginación para buckets con gran cantidad de objetos.
- 📝 Registro de eventos (logging) durante todo el proceso de descarga.

---

## 🛠️ Tecnologías

| Tecnología | Versión |
|---|---|
| .NET | 6.0 |
| AWSSDK.S3 | 3.7.x |
| Microsoft.Extensions.Hosting | 6.0.1 |

---

## ✅ Requisitos previos

- [.NET 6 SDK](https://dotnet.microsoft.com/en-us/download/dotnet/6.0)
- Una cuenta de AWS con acceso a S3.
- Un bucket de S3 existente con los archivos a descargar.
- Credenciales de AWS (**Access Key** y **Secret Key**) con permisos de lectura sobre el bucket (`s3:GetObject`, `s3:ListBucket`).

---

## ⚙️ Configuración

Edita el archivo `appsettings.json` y completa la sección `AWS` con tus credenciales y datos del bucket:

```json
{
  "Logging": {
    "LogLevel": {
      "Default": "Information",
      "Microsoft.Hosting.Lifetime": "Information"
    }
  },
  "AWS": {
    "AccessKey": "TU_ACCESS_KEY",
    "SecretKey": "TU_SECRET_KEY",
    "BucketName": "nombre-de-tu-bucket",
    "Region": "us-east-1"
  }
}
```

> ⚠️ **Importante:** No publiques tus credenciales de AWS en repositorios públicos. Considera usar [AWS Secrets Manager](https://aws.amazon.com/secrets-manager/) o variables de entorno para proteger tus credenciales.

### Directorio de destino

El directorio local donde se guardan los archivos descargados está definido en `Worker.cs`:

```csharp
string destinationPath = @"D:\Fotos";
```

Modifica esta ruta según el sistema operativo y la ubicación deseada.

---

## 🚀 Instalación y ejecución

1. **Clona el repositorio:**

```bash
git clone https://github.com/MatosSecceMarcoAntonio/AwsDownload-S3-Files.git
cd AwsDownload-S3-Files
```

2. **Restaura las dependencias:**

```bash
dotnet restore
```

3. **Configura tus credenciales** en `appsettings.json` (ver sección de configuración).

4. **Ejecuta el servicio:**

```bash
dotnet run
```

---

## 🏗️ Estructura del proyecto

```
AwsDownload-S3-Files/
├── Program.cs                  # Punto de entrada, configura el host genérico
├── Worker.cs                   # Lógica principal del servicio en segundo plano
├── appsettings.json            # Configuración de la aplicación (credenciales AWS, bucket)
├── appsettings.Development.json
├── AwsDownloadFIle.csproj      # Archivo de proyecto .NET
└── AwsDownloadFIle.sln         # Solución de Visual Studio
```

---

## 🔄 Flujo de funcionamiento

```
Inicio del servicio
       │
       ▼
Listar objetos del bucket S3 (con paginación)
       │
       ▼
Por cada objeto:
  ┌─────────────────────────────────────────┐
  │ ¿Es una carpeta?  →  Crear carpeta local│
  │ ¿Archivo existe y mismo tamaño? → Omitir│
  │ En otro caso → Descargar archivo        │
  └─────────────────────────────────────────┘
       │
       ▼
Esperar 1 hora y repetir
```

---

## 📄 Licencia

Este proyecto está disponible de forma abierta. Puedes usarlo y modificarlo libremente.
 

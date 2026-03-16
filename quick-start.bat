@echo off
REM ═══════════════════════════════════════════════════════════════════
REM quick-start.bat — Setup automático para Windows
REM
REM Este script levanta el proyecto completo con un solo comando.
REM Requiere: Docker Desktop instalado y ejecutándose.
REM ═══════════════════════════════════════════════════════════════════

setlocal enabledelayedexpansion

set "ROOT_DIR=%~dp0"
cd /d "%ROOT_DIR%"

echo ════════════════════════════════════════════════════════════════
echo   TESIS BENCHMARK — Quick Start (Windows)
echo ════════════════════════════════════════════════════════════════
echo.

REM Verificar Docker
docker info >nul 2>&1
if %ERRORLEVEL% neq 0 (
    echo [ERROR] Docker no está ejecutándose.
    echo.
    echo Por favor:
    echo   1. Abre Docker Desktop
    echo   2. Espera a que diga "Docker is running"
    echo   3. Vuelve a ejecutar este script
    echo.
    pause
    exit /b 1
)

echo [OK] Docker está ejecutándose
echo.

REM Verificar si ya está configurado
if exist ".env" (
    echo [OK] Archivo .env encontrado
) else (
    if exist ".env.example" (
        echo [OK] Creando .env desde .env.example...
        copy .env.example .env
    )
)

echo.
echo ════════════════════════════════════════════════════════════════
echo   Compilando jobs Java (primera vez solo)...
echo ════════════════════════════════════════════════════════════════

if not exist "lib\batch-job.jar" (
    echo [INFO] Compilando JARs...
    call gradlew.bat buildJobs
    if %ERRORLEVEL% neq 0 (
        echo [ERROR] Falló la compilación
        pause
        exit /b 1
    )
    
    REM Copiar JARs a lib/
    if not exist "lib" mkdir lib
    copy /Y "batch\out\libs\batch-job.jar" "lib\" >nul
    copy /Y "microbatch\out\libs\microbatch-job.jar" "lib\" >nul
    copy /Y "streaming\out\libs\streaming-job.jar" "lib\" >nul
) else (
    echo [OK] JARs pre-compilados encontrados
)

echo.
echo ════════════════════════════════════════════════════════════════
echo   Levantando infraestructura Docker...
echo ════════════════════════════════════════════════════════════════

docker compose build --pull missing generator probe
if %ERRORLEVEL% neq 0 (
    echo [ERROR] Falló la construcción de imágenes
    pause
    exit /b 1
)

docker compose up -d --no-build
echo.

REM Esperar a que Kafka esté disponible
echo [INFO] Esperando a Kafka...
:wait_kafka
timeout /t 2 /nobreak >nul
docker compose exec -T kafka kafka-broker-api-versions --bootstrap-server localhost:9092 >nul 2>&1
if %ERRORLEVEL% neq 0 (
    goto wait_kafka
)
echo [OK] Kafka disponible

REM Crear tópico
docker compose exec -T kafka kafka-topics --create --topic events --partitions 12 --replication-factor 1 --bootstrap-server localhost:9092 --if-not-exists >nul 2>&1

echo.
echo ════════════════════════════════════════════════════════════════
echo   ¡Setup completado!
echo ════════════════════════════════════════════════════════════════
echo.
docker compose ps
echo.
echo   Servicios disponibles:

echo   • Spark UI:   http://localhost:8080
echo   • Flink UI:   http://localhost:8081
echo.
echo   Próximos pasos:
echo   • Validar:   bash .\scripts\doctor.sh
echo   • Probar:   bash .\scripts\run_batch.sh low-load run_1
echo   • Rápido:   bash .\scripts\run_experiment.sh --quick
echo.
pause

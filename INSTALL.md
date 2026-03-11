# Guía de Instalación — Tesis Benchmark

Esta guía te permite levantar el proyecto en cualquier PC con Windows, macOS o Linux.

## Requisitos del Sistema

| Componente | Mínimo | Recomendado |
|------------|--------|-------------|
| RAM | 8 GB | 16 GB |
| CPU | 4 núcleos | 8+ núcleos |
| Disco | 20 GB libres | 50 GB SSD |
| Docker | Docker Desktop 4.x+ | Docker Desktop 8GB+ |

> **Nota:** Docker Desktop debe tener asignados al menos 8 GB de RAM en la configuración de recursos.

---

## Instalación Paso a Paso

### 1. Windows

#### Opción A: Con WSL2 (Recomendado)

```powershell
# 1. Instalar WSL2 si no lo tienes:
wsl --install

# 2. Abrir Ubuntu (o cualquier distribución) y ejecutar:
cd /mnt/c/Users/TU_USUARIO/Documents/Tesis/data-ingestion-strategies
bash ./scripts/setup_minimal.sh
```

#### Opción B: Con Git Bash

```bash
# 1. Instalar Git for Windows (incluye Git Bash)
# 2. Abrir Git Bash y ejecutar:
cd /c/Users/TU_USUARIO/Documents/Tesis/data-ingestion-strategies
bash ./scripts/setup_minimal.sh
```

#### Opción C: Quick Start (Windows)

```powershell
# Doble clic en quick-start.bat o ejecutar en PowerShell:
.\quick-start.bat
```

### 2. macOS

```bash
# 1. Instalar Docker Desktop: https://www.docker.com/products/docker-desktop

# 2. Abrir Terminal y ejecutar:
cd ~/Documents/Tesis/data-ingestion-strategies
bash ./scripts/setup_minimal.sh
```

### 3. Linux (Ubuntu/Debian)

```bash
# 1. Instalar Docker:
sudo apt update
sudo apt install docker.io docker-compose

# 2. Iniciar Docker:
sudo systemctl start docker
sudo systemctl enable docker

# 3. Ejecutar setup:
cd ~/Documents/Tesis/data-ingestion-strategies
bash ./scripts/setup_minimal.sh
```

---

## Verificación Post-Instalación

```bash
# Ver que todos los servicios estén corriendo
docker compose ps

# Ejecutar checks de salud
bash ./scripts/doctor.sh
```

Deberías ver todos los servicios en estado "healthy" o "running".

---

## Primeros Pasos

### 1. Prueba rápida (1 estrategia, 1 escenario)

```bash
# Batch (5 minutos por defecto)
bash ./scripts/run_batch.sh low-load run_1

# Microbatch
bash ./scripts/run_microbatch.sh low-load run_1 "5 seconds"

# Streaming
bash ./scripts/run_streaming.sh low-load run_1
```

### 2. Experimento completo (reducido)

```bash
# Rápido: 3 estrategias × 2 escenarios × 1 repetición (~30 min total)
bash ./scripts/run_experiment.sh --quick

# Personalizado:
bash ./scripts/run_experiment.sh --reps 2 --duration 300 --scenarios "low-load medium-load"
```

### 3. Análisis de resultados

```powershell
# Windows
analysis\.venv\Scripts\python.exe analysis\analyze.py

# Linux/macOS
source analysis/.venv/bin/activate
python analysis/analyze.py
```

---

## Solución de Problemas

### "Docker daemon is not available"

```powershell
# Iniciar Docker Desktop y esperar a que diga "Docker is running"
```

### "Java not found" al compilar

El proyecto incluye JARs pre-compilados en `lib/`. Si necesitas recompilar:

```bash
# Windows (PowerShell)
.\gradlew.bat buildJobs

# Linux/macOS
./gradlew buildJobs
```

### "Port already in use"

```bash
# Ver qué proceso usa el puerto
netstat -ano | findstr :9090  # Windows
lsof -i :9090                 # Linux/macOS

# Cambiar puerto en .env si es necesario
```

### Microbatch/Streaming fallan en cargas altas

1. Aumentar RAM asignada a Docker Desktop (12-16 GB)
2. Reducir escala del escenario:
   ```bash
   # En lugar de extreme-load (100k/s), usar high-load (30k/s)
   bash ./scripts/run_batch.sh high-load run_1
   ```

### Ver logs en tiempo real

```bash
# Todos los servicios
docker compose logs -f

# Solo un servicio
docker compose logs -f generator
docker compose logs -f flink-jobmanager
```

---

## Estructura de Archivos Clave

```
├── lib/                    # JARs pre-compilados (no necesitas Gradle)
├── scripts/
│   ├── setup_minimal.sh   # Setup automático
│   ├── run_batch.sh       # Ejecutar estrategia Batch
│   ├── run_microbatch.sh  # Ejecutar Microbatch
│   ├── run_streaming.sh  # Ejecutar Streaming
│   └── run_experiment.sh # Experimento completo
├── analysis/
│   ├── analyze.py        # Análisis estadístico
│   └── requirements.txt  # Dependencias Python
└── results/              # Donde se guardan los CSVs y métricas
```

---

## ¿Necesitas Ayuda?

1. Revisa `docs/architecture.md` para entender el diseño
2. Revisa `docs/experiment_protocol.md` para el protocolo experimental
3. Ejecuta `bash ./scripts/doctor.sh` para diagnóstico automático

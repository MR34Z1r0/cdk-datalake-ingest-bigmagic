branch=$(git rev-parse --abbrev-ref HEAD)

if [[ "$branch" == "dev" ]]; then
    echo "Activando entorno de desarrollo (.venv-dev)"
    cp dev.env .env
elif [[ "$branch" == "master" ]]; then
    echo "Activando entorno de producción (.venv-prod)"
    cp prod.env .env
else
    echo "Rama '$branch' no reconocida, activa el entorno manualmente."
fi

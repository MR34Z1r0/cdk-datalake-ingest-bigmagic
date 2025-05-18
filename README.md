
# Welcome to your CDK Python project!

This is a blank project for CDK development with Python.

The `cdk.json` file tells the CDK Toolkit how to execute your app.

This project is set up like a standard Python project.  The initialization
process also creates a virtualenv within this project, stored under the `.venv`
directory.  To create the virtualenv it assumes that there is a `python3`
(or `python` for Windows) executable in your path with access to the `venv`
package. If for any reason the automatic creation of the virtualenv fails,
you can create the virtualenv manually.

To manually create a virtualenv on MacOS and Linux:

Si no se reconoce la palabra clave python
```
export PATH="/c/ProgramData/anaconda3:/c/ProgramData/anaconda3/Scripts:$PATH"
```


```
$ python -m venv .venv
```

After the init process completes and the virtualenv is created, you can use the following
step to activate your virtualenv.

```
$ source .venv/Scripts/activate
$ source .venv/bin/activate
```

If you are a Windows platform, you would activate the virtualenv like this:

```
% .venv\Scripts\activate.bat
```

Once the virtualenv is activated, you can install the required dependencies.

```
$ pip install -r requirements.txt
```

At this point you can now synthesize the CloudFormation template for this code.

```
$ cdk synth
```

To add additional dependencies, for example other CDK libraries, just add
them to your `setup.py` file and rerun the `pip install -r requirements.txt`
command.

## Useful commands

 * `cdk ls`          list all stacks in the app
 * `cdk synth`       emits the synthesized CloudFormation template
 * `cdk deploy`      deploy this stack to your default AWS account/region
 * `cdk diff`        compare deployed stack with current state
 * `cdk docs`        open CDK documentation

Enjoy!

# Nombre de tu Proyecto

## Flujo de trabajo para desarrollo y despliegue

Este proyecto utiliza GitHub Actions para automatizar el despliegue a los entornos de desarrollo y producción.

### Entornos

- **Desarrollo (DEV)**: Asociado a la rama `dev`
- **Producción (PRD)**: Asociado a la rama `master`

### Reglas del flujo de trabajo

1. **Desarrollo diario:**
   - Trabaja en la rama `dev` o en ramas de características (`feature/nombre`)
   - Puedes hacer push directamente a `dev` para un despliegue automático a desarrollo
   - Los cambios en `dev` se despliegan automáticamente al entorno de desarrollo

2. **Lanzamiento a producción:**
   - **NUNCA** hagas push directamente a la rama `master`
   - Todos los cambios a producción deben pasar por un Pull Request desde `dev` a `master`
   - El PR debe ser revisado y aprobado por al menos un miembro del equipo
   - Una vez aprobado, haz merge del PR (no uses push)
   - Los cambios se desplegarán automáticamente a producción

3. **Para hotfixes urgentes:**
   - Crea una rama de hotfix desde `master`: `git checkout -b hotfix/descripcion master`
   - Implementa la corrección y crea un PR desde tu rama de hotfix a `master`
   - Después de desplegar a producción, no olvides integrar el fix a `dev`
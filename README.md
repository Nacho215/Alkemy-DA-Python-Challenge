# Introduction
This is my project for the [Alkemy Data Analytics + Python Challenge](https://campus.alkemy.org/challenges/27293).
# Setup
## For Windows
If you are using Windows PowerShell, you need to open a PS console as Administrator and run:

```
Set-ExecutionPolicy Unrestricted
```

Open a terminal inside the project folder and run the following commands to create a virtual enviroment, activate it, and install the required dependencies:

```
python -m venv venv
venv/Scripts/Activate.ps1
pip install -r requirements.txt
```

## For Linux
Open a terminal inside the project folder and run the following commands to create a virtual enviroment, activate it, and install the required dependencies:

```
python -m venv venv
source venv/Scripts/activate
pip install -r requirements.txt
```

## For both Windows and Linux

This part it's the same for both Windows and Linux.

Now connect to PostgreSQL with the command:

```
psql postgres postgres
```

If ask you for a password, pass it something like 'postgres' or 'admin'.

Once you are connected, create an user:

```
CREATE USER alkemy with encrypted password 'alkemy';
```

And allow him to create databases:

```
ALTER USER alkemy CREATEDB;
```

That's all! You can now run the program by executing *challenge.py* file:

```
python src/challenge.py
```
# pipeutils

Set of libraries to work with pipelines

## documentation 

```
$sphinx-build -b html src build

```

## Local setup

```
$wget --no-check-certificate -O $HOME/miniconda.sh https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh 
$bin/bash $HOME/miniconda.sh -b -p $HOME/miniconda
```

```
$conda create --name pipelines python=3
$source activate pipelines
$pip install -r requirements.txt
 ór
$pip install -e .
```

```
$python setup.py install

```

## Docker setup

### Build

Clone the repository:

```
$git clone https://github.com/codebolab/pipeutils.git
$cd pipeutils
```

### Build the image:

```
$docker build -t pipeutils .
```

### Run container


```
$docker run --rm -ti --name pipeutils pipeutils /bin/bash
#cd /app/
#python setup.py install 
```


### Google API Client Libraries Python

Setup steps you need to complete before you can use this library:

1. If you don't already have a Google account, [sign up.](https://www.google.com/accounts)
2. If you have never created a Google APIs Console project, read the [Managing Projects page](https://developers.google.com/console/help/managing-projects) and create a project in the [Google API Console.](https://console.developers.google.com/)

[Turn on the Drive API](https://developers.google.com/drive/api/v3/quickstart/python)

This opens a new dialog. In the dialog, do the following:
* Select + Create a new project.
* Download the configuration file.
* Move the downloaded file to your path config into GDRIVE['secret_file'] directory and ensure it is named gdrive.conf

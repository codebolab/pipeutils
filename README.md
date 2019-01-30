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

Build the image:


### Run container


```
$docker run --rm -ti --name pipeutils pipeutils /bin/bash
#cd /app/
#python setup.py install 
```

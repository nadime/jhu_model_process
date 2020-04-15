#!/bin/sh

SCRIPT_DIR=$(dirname "$0")
INPUT_URL="$1"
INPUT_URL_LEN=${#INPUT_URL}

if [[ $INPUT_URL_LEN -lt 5 ]]
then
  echo Error: $INPUT_URL does not look right - should be s3 URL
  exit 1
fi

# get python3
sudo yum -y install python3

# install needed packages
/usr/bin/pip3 install --user -r $SCRIPT_DIR/../config/requirements.txt

if [[ -d /data ]]
then
  echo Removing /data
  sudo rm -rf /data
fi

# mount stuff
sudo mkfs -t xfs /dev/nvme1n1

sudo mkdir /data

sudo mount /dev/nvme1n1 /data

sudo chown $USER:$USER /data

ln -s /data ~/
mkdir ~/data/temp

# get input data from s3
mkdir ~/data/input
mkdir ~/data/input/now

FILE_NAME=$(basename "$INPUT_URL")
if [[ ! -f ~/data/temp/$FILE_NAME ]]
then
  echo Getting $FILE_NAME from $INPUT_URL
  aws s3 cp $INPUT_URL ~/data/temp
  RESULT="$?"
  if [ "$RESULT" -ne 0 ]
  then
    echo "Error: failed to get $INPUT_URL from s3, are your credentials set?"
    exit 1
  fi
else
  echo ~/data/temp/$FILE_NAME already exists
fi

if [ ${FILE_NAME: -3} == ".gz" ]
then
  tar -C ~/data/input/now -xzvf ~/data/temp/$FILE_NAME
elif [ ${FILE_NAME: -4} == ".bz2" ]
then
  tar -C ~/data/input/now -xvf ~/data/temp/$FILE_NAME
else
  echo "Error: did not understand input file extension ${FILE_NAME: -3} from $FILE_NAME" 1>&2
  exit 1
fi

RESULT="$?"
if [ "$RESULT" -ne 0 ]
then
  echo "Error: failed to unpackage ~/data/temp/$FILE_NAME"
  exit 1
fi

echo Done!

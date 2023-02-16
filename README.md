# DE Challenge GPS Data

## Installation
```
pip install -r requirements.txt
```
## Validating bash file is executable
```
chmod +x start.sh
```
## Crontab config
```
sudo crontab -e
```
Add to file:
```
0 0 * * * start.sh
```

# tumblr-backup
Backup a tumblr blog to a local SQLite database using the Tumblr API.  
**This does not include downloading images**. For that, check out [tumblr-dl](https://github.com/spambusters/tumblr-dl).  

## Requirements  
1. Python 3.6  
2. requests library `pip install requests`  

## Usage  
```
tumblr-backup.py [-h] [-o OFFSET] blog

positional arguments:
  blog                  Tumblr blog

optional arguments:
  -o, --offset  Post offset
```  

## Config  
This script uses the Tumblr API so you'll need an API key.  
This can be obtained by using the [Tumblr API Console](https://api.tumblr.com/console).  

Next, create a config file. (e.g. `config.txt`).  
The API key should be the first and only line of the config file.  

Finally, insert the PATH of the config into the `CONFIG` variable of tumblr-backup.py.  

```CONFIG = '../config.txt'```

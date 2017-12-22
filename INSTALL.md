Der "Politik bei uns"-Daemon hat zwei Betriebsmodi, welcher über den Konfigurationsparameter `ENABLE_PROCESSING` gesteuert wird:
* OParl-only-Modus, in dem alle Daten abgerufen und ggf. korrigiert werden, jedoch aber keine Weiterverarbeitung stattfindet
* Weiterverarbeitungs-Modus, in dem alle Daten abgerufen und weiterverarbeitet werden

Der OParl-only-Modus benötigt folgende Komponenten:
* Ein Linux (getestet mit Ubuntu 16.04 und Debian 9.0)
* Python 3 (getestet mit Python 3.5)
* MongoDB 3 (getestet mit MongoDB 3.2 und 3.4)
* Minio

Der Weiterverarbeitungs-Modus benötigt darüber hinaus:
* ElasticSearch 5 (getestet mit ElasticSearch 5.6)
* ghostscript (getestet mit ghostscript 9.18)
* pdftotext (getestet mit pdftotext 0.41)
* abiword (getestet mit abiword 3.0.1)
* jpegoptim (getestet mit jpegoptim 1.4.3)

Um den Daemon zu installieren, brauchen wir zunächst die Dateien

```bash
$ mkdir daemon
$ cd daemon
$ git clone https://github.com/politik-bei-uns/daemon.git .
```

Anschließend benötigen wir ein Virtual Environment und alle Pakete:
```bash
$ virtualenv -p python3 venv 
$ source venv/bin/activate
$ pip install -r requirements.txt
```

Des weiteren muss die Konfigurationsdatei erstellt werden:
```
$ cp oparlsync/config-dist.py oparlsync/config.py
$ vim oparlsync/config.py
```

Anschließend kann der Daemon verwendet werden:
```
$ python manage.py
```

Wenn man die SSH-Verbindung geschlossen hat, muss man immer erst wieder in das Virtual Enviroment zurück und kann dann wie gewohnt weiterarbeiten:
```
$ source venv/bin/activate
$ python manage.py
```
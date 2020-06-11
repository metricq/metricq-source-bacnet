# BACnet test device for local testing

Change ip in bacpypes.ini to your local ip, then run:

```
python BacnetTestDevice.py --ini bacpypes.ini
```

For simple debugging append ```--debug```. For more availabel objects set ```RANDOM_OBJECT_COUNT``` env var.
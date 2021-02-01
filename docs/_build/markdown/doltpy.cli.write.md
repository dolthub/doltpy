# doltpy.cli.write package

## Submodules

## doltpy.cli.write.write module


### doltpy.cli.write.write.write_columns(dolt: doltpy.cli.dolt.Dolt, table: [str](https://docs.python.org/3/library/stdtypes.html#str), columns: Mapping[[str](https://docs.python.org/3/library/stdtypes.html#str), List[Any]], import_mode: Optional[[str](https://docs.python.org/3/library/stdtypes.html#str)] = None, primary_key: Optional[List[[str](https://docs.python.org/3/library/stdtypes.html#str)]] = None, commit: Optional[[bool](https://docs.python.org/3/library/functions.html#bool)] = False, commit_message: Optional[[str](https://docs.python.org/3/library/stdtypes.html#str)] = None, commit_date: Optional[[datetime.datetime](https://docs.python.org/3/library/datetime.html#datetime.datetime)] = None)

* **Parameters**

    
    * **dolt** – 


    * **table** – 


    * **columns** – 


    * **import_mode** – 


    * **primary_key** – 


    * **commit** – 


    * **commit_message** – 


    * **commit_date** – 



* **Returns**

    


### doltpy.cli.write.write.write_file(dolt: doltpy.cli.dolt.Dolt, table: [str](https://docs.python.org/3/library/stdtypes.html#str), file_handle: _io.StringIO, filetype: [str](https://docs.python.org/3/library/stdtypes.html#str) = 'csv', import_mode: Optional[[str](https://docs.python.org/3/library/stdtypes.html#str)] = None, primary_key: Optional[List[[str](https://docs.python.org/3/library/stdtypes.html#str)]] = None, commit: Optional[[bool](https://docs.python.org/3/library/functions.html#bool)] = False, commit_message: Optional[[str](https://docs.python.org/3/library/stdtypes.html#str)] = None, commit_date: Optional[[datetime.datetime](https://docs.python.org/3/library/datetime.html#datetime.datetime)] = None)

### doltpy.cli.write.write.write_pandas(dolt: doltpy.cli.dolt.Dolt, table: [str](https://docs.python.org/3/library/stdtypes.html#str), df: pandas.core.frame.DataFrame, import_mode: Optional[[str](https://docs.python.org/3/library/stdtypes.html#str)] = None, primary_key: Optional[List[[str](https://docs.python.org/3/library/stdtypes.html#str)]] = None, commit: Optional[[bool](https://docs.python.org/3/library/functions.html#bool)] = False, commit_message: Optional[[str](https://docs.python.org/3/library/stdtypes.html#str)] = None, commit_date: Optional[[datetime.datetime](https://docs.python.org/3/library/datetime.html#datetime.datetime)] = None)

* **Parameters**

    
    * **dolt** – 


    * **table** – 


    * **df** – 


    * **import_mode** – 


    * **primary_key** – 


    * **commit** – 


    * **commit_message** – 


    * **commit_date** – 



* **Returns**

    


### doltpy.cli.write.write.write_rows(dolt: doltpy.cli.dolt.Dolt, table: [str](https://docs.python.org/3/library/stdtypes.html#str), rows: List[[dict](https://docs.python.org/3/library/stdtypes.html#dict)], import_mode: Optional[[str](https://docs.python.org/3/library/stdtypes.html#str)] = None, primary_key: Optional[List[[str](https://docs.python.org/3/library/stdtypes.html#str)]] = None, commit: Optional[[bool](https://docs.python.org/3/library/functions.html#bool)] = False, commit_message: Optional[[str](https://docs.python.org/3/library/stdtypes.html#str)] = None, commit_date: Optional[[datetime.datetime](https://docs.python.org/3/library/datetime.html#datetime.datetime)] = None)

* **Parameters**

    
    * **dolt** – 


    * **table** – 


    * **rows** – 


    * **import_mode** – 


    * **primary_key** – 


    * **commit** – 


    * **commit_message** – 


    * **commit_date** – 



* **Returns**

    

## Module contents

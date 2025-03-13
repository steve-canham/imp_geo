<h2>Introduction and Purpose</h2>
A utility intended for periodic imports of Geonames data, processing that data and making a subset of it available for other systems.
The geonames data is regularly updated, and provides a great resource for identifying cities and countries after they have been input 
as free text into systems, perhaps in a variety of languages.

Data is stored in a Postgres database called 'geo'. <br/>
The system first imports the geonames data to a 'src' schema, from a variety of source files, creating tables with matching names (see Downloading the Data below).<br/> 
It then uses that data to create 5 main tables of data, in a schema called 'geo':
<ul>
<li>countries - basic data on names and codes for each of about 250 listed countries. Those with a population of over 320,000 are ranked 1, the rest are ranked as 2.</li> 
<li>country_names - for each country, the various names under which they are known in the Geonames system, including - in most cases - the language(s) of each name.</li>
<li>cities - basic data on names and codes for each of about 60,000 listed cities with a population of 5000 or more. Names and codes are also provided of the city's country and top administrative district.</li> 
<li>city_names - for each city, the various names under which they are known in the Geonames system, including - in most cases - the language(s) of each name.</li>
<li>regions - a subset of the geonames 'non-country' data listing the main regional, geopolitical, linguistic and continental groupings of countries. Such scopes can be used to support queries that target broader areas than individual countries.</li> 
</ul>
A few minor changes are made to the data in the geo tables, to make it more accurate. In particular a) Serbia and Montenegro is removed from the countries list (it ceased to exist in 2006), 
b) Hong Kong and Macau are removed from the countries list, as they are increasingly integrated into mainland China, and c) Cities listed as being in Hoing Kong and Macao are transferred to China 
(though retain their original admin area codes). 

The final stage is to create a set of tables in a schema called 'mdr'. This has tables designed to be used to match and code city and country data in the MDR system, where it is often input 
as free text and therefore in a wide variety of spellings. Some tables therefore incorporating additional name variations derived from the MDR system. The tables are 
<ul>
<li>country_names - for each country, the various names under which they are known in the Geonames and MDR systems, along with ISO codes and continent codes. A key field is 'comp_name', a lowercase version of each name, shorn of periods, designed  
to be used for comparison and identification purposes.</li>
<li>city_names - for each city, the various names under which they are known in the Geonames system, plus additional names for some cities identified within the MDR. Again a 'comp_name' field provides a lower case version for 
comparison and identification purposes.</li>
<li>geo_scopes - an augmented import of the geo.regions data, with additional information about the 'parents' and 'members' of each scope. This data represents the various 'geographic scopes' that some organisations work within, 
or are designed to support. This is available for use within the mdr.</li> 
<li>scope_membership - for each scope, provides the member countries as a set of records.</li> 
</ul>


<h2>How to use the system  (notes to self)</h2>

<h3>Downloading the data</h3>
The data is obtained from GeoNames, based at <a href="https://www.geonames.org/" target="_blank">https://www.geonames.org</a>, which is described as 
<i>‘a user-editable geographical database available and accessible through various web services, under a Creative Commons attribution license’, founded in late 2005. 
As of 2025, GeoNames included ‘over 25 million geographical names and consists of over 12 million unique features whereof 4.8 million populated places and 18 million alternate names.'</i>

First create a folder to receive the data files, e.g. 'E:\MDR source data\Geonames\data\Geonames 20250205’. 
Then go to <a href="https://download.geonames.org/export/dump/" target="_blank">https://download.geonames.org/export/dump/</a>, which provides a long list of GeoNames source files, as well as information (below the list) about file structure. 
The following .txt files are required:
<ul>
<li>admin1CodesASCII.txt</li> 
<li>admin2Codes.txt</li> 
<li>countryInfo.txt</li> 
<li>iso-languagecodes.txt</li> 
</ul>
Clicking these will open them directly in a browser. They can then be ‘Saved As…’ a file with the same name in the source data folder constructed above. 

The following .zip files are also  required
<ul>
<li>cities5000.zip</li> 
<li>no-country.zip</li> 
<li>alternatenamesV2.zip</li> 
</ul>
These should be downloaded and their contents extracted into .txt files, with those files also transferred to the source folder. 
Any comments above the data should be removed, (this mainly applies to countryInfo.txt), as should any top rows with field names (with the exception of iso-languagecodes.txt, where a header row is expected). 
Note that these file names have been constant for several years and are expected to remain so. The names are hard-wired into the system - if they do change the code will require matching changes.

<h3>Configuration</h3>
The system requires a simple configuration file (app_config.toml) in the same folder as cargo.toml. This needs to have the structure shown below, 
with the relevant values inserted between the double quotes: <br/>
<br/>
[folders]<br/>
data_folder_path=""<br/>
log_folder_path=""<br/>
<br/>
[database]<br/>
db_host=""<br/>
db_user=""<br/>
db_password=""<br/>
db_port=""<br/>
db_name=""<br/>
<br/>
<h3>Usage</h3>
There is no initialisation required - other than the provision of an empty database called 'geo'. The system recreates all schemas and tables from scratch each time it is run.<br/>
'cargo run' will create the data tables described above, assuming the listed source files are all in the specified data folder. A simple log is generated.<br/>
Each run takes about 2 minutes, about half that if 'cargo run -r' (i.e. the release build) is used. Almost all of that time is spent importing and processing the data in the 
alternatenamesV2 file, as this has over 18,000,000 rows.<br/>

There is only one flag available to the user. -n ('cargo run -- -n') will include non-latin names in the alternate names, that are used to create the country_names and city_names tables. 
By default non-latin names are excluded as in many use cases, at least in Europe, they would not be meaningful or offered to the user. Using the -n flag makes both the names tables about 
25% larger and makes the process a little slower, though it still takes just a few minutes.

The additional names derived from the MDR are added manually as part of the code (as simple insert statements). This keeps all the relevant data in one place, rather than having to 
maintain and load separate files. It is the responsibility of client systems, such as the MDR, to identify geographical entities that are not matched by the data, so that the code 
can be periodically updated (probably every three to six months). In use, the mdr schema tables are designed to be imported as foreign tables into the database(s) making use of them.


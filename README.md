<h2>Introduction and Purpose</h2>
A utility intended for periodic imports of Geonames data, processing that data and making a subset of it available for other systems.
The geonames data is regularly updated, and providees a great resource for identifying cities and countries after they have been input 
as free text into systems, perhaps in a variety of languages.

Data is stored in a Postgres database called 'geo'. <br/>
The system first imports the geonames data to a geo schema, from a variety of source files, and uses that data to create 6 main tables of data, in a schema called 'loc', in a database called 'geo':
<ul>
<li>countries - basic data on names and codes for each of about 250 listed countries. Thosde with a population of over 320,000 are ranked 1, the rest are ranked as 2.</li> 
<li>country_names - for each country, the various names under which they are known in the Geonames system, including - in most cases - the language(s) of each name.</li>
<li>cities - basic data on names and codes for each of about 60,000 listed cities with a population of 5000 or more. Names and codes are also provided of the city's country and top administrative district.</li> 
<li>city_names - for each city, the various names under which they are known in the Geonames system, including - in most cases - the language(s) of each name.</li>
<li>scopes - regional, geopolitical linguistic and continental groupings of countries. Such scopes can ne used to support queries that target broader areas than individual countries.</li> 
<li>scope_membership - for each composite scope, a listing of the countries that are members of that scope.</li>
</ul>
The loc tables are designed to be imported into, other systems such as the MDR, there often integrated with country and city data or names that is specific to the host system.

<h2>How to use it  (notes to self)</h2>

<h3>Download the data</h3>
The data is obtained from GeoNames, based at <a href="https://www.geonames.org/" target="_blank">https://www.geonames.org</a>, which is described as 
‘a user-editable geographical database available and accessible through various web services, under a Creative Commons attribution license’, founded in late 2005. 
As of 2025, GeoNames included ‘over 25 million geographical names and consists of over 12 million unique features whereof 4.8 million populated places and 18 million alternate names.'

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
Any comments above the data should be removed, (mainly applies to countryInfo.txt), plus any top rows with field names (with the exception of the iso-languagecodes file, where a header row is expected). 
Note that these file names have been constant fore several years and are expected to remain so. They are hard-wired into the system - if they do change the code will require matching changes.

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
There is no initialisation required - the system recreates all tables from scratch each time it is run.<br/>
'cargo run' will create the data tables described above, assuming the listed source files are all in the specified data folder. A simple log is generated.<br/>
Each run takes about 2 minutes. Almost all of that time is spent importing and processing the data in the alternatenamesV2 file, as this has over 198,000,000 rows.<br/>

There is only one flag available to the user. -n ('cargo run -- -n') will include non-latin names in the alternate names, that are used to create the country_names and city_names tables. 
By default non-latin names are excluded as in many use cases, at least in Europe, they would not be meaningful or offered to the user. Using the -n flag makes both the names tables about 
25% larger and makes the process a little slower, though it still takes just a few minutes.


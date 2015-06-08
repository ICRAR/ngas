<?xml version="1.0" encoding="utf-8"?>
<resource schema="mwa">
<meta name="title">GLEAM Images</meta>
<meta name="copyright" format="plain">
GLEAM Cutout Service: The GaLactic and Extragalactic MWA Survey Cutout Service
</meta>
<meta name="creationDate">2015-06-06T12:00:00Z</meta>
<meta name="description">The GLEAM cutout service provides mosaic cutout interface to the GLEAM science user community.</meta>
<meta name="subject">Gleam cutout</meta>
<meta name="coverage.waveband">Radio</meta>
<meta name="creator"></meta>
<meta name="logo">http://ict.icrar.org/store/staff/biqing/GLEAM-logo.png</meta>
<meta name="_related" title="GLEAM Web Site">http://www.mwatelescope.org/index.php/science/galactic-and-extragalactic-surveys</meta>
<meta name="_bottominfo" format="raw">
      	<![CDATA[
				<img src="http://ict.icrar.org/store/staff/biqing/GLEAM-logo.png" height="42" width="160"/>
			]]>
</meta>

<table id="gleam_mosaic" adql="True" onDisk="True" mixin="//products#table">
  <stc>
   Polygon ICRS [coverage]
   Position "centerAlpha" "centerDelta"
  </stc>

  <column name="centerAlpha"
    ucd="pos.eq.ra;meta.main" type="real"
    description="Right ascension of object observed"/>

  <column name="centerDelta"
    ucd="pos.eq.dec;meta.main" type="real"
    description="Declination of object observed"/>

  <column name="coverage" type="sbox" unit="deg" description="Rectangle covered by the image"/>
  <index columns="coverage" name="pgspos" method="GIST"/>

  <column name="center_freq"  type="double precision" unit="MHz" tablehead="Center frequency"
      ucd="instr.cen.freq" description="central frequency of the data on the image. Min 72, Max 250 MHz.">
    <values nullLiteral="0"/>
  </column>

  <column name="robustness"  type="integer" tablehead="Briggs robustness"
			description="Briggs weighting scheme">
      <values nullLiteral="-1"/>
	</column>
  <column name="filename"  type="text" tablehead="File Download"
			description="The name of archived file in NGAS"
	/>
</table>

<rowmaker id="make_mwa">
	<map key="center_freq">int(@CRVAL3/1e6)</map>
  <map key="filename">"empty"</map>

	<apply name="addCoverage">
		 <code>
			wcs=coords.getWCS(vars, naxis=(1,2))
			result["centerAlha"], result["centerDelta"] = coords.getCenterFromWCSFields(wcs)
			radius_in_degrees = 20
			result["coverage"] = pgsphere.SCircle(pgsphere.SPoint.fromDegrees(result["centerAlpha"], result["centerDelta"]),radius_in_degrees*DEG)
		 </code>
	</apply>
</rowmaker>

<data id="content">
	<sources pattern="/var/gavo/inputs/mwa/*.zhl" recurse="True"/>
	<fitsProdGrammar qnd="True">
			<rowfilter procDef="//products#define">
				<setup><code>
					<![CDATA[
					def getEmbargo(row):
						res = parseISODT(row["DATE_OBS"])
						return res
					]]>
				</code></setup>
				<bind key="owner">"ICRAR"</bind>
				<bind key="embargo">getEmbargo(row)</bind>
				<bind key="table">"mwa.gleam_mosaic"</bind>
		</rowfilter>
  </fitsProdGrammar>
  <make table="gleam_mosaic" rowmaker="make_mwa"/>
</data>


<dbCore id="gleam_cutout" queriedTable="gleam_mosaic">
  <condDesc combining="True">
	  <inputKey name="pos" type="text"
	  	multiplicity="single"
		  description= "SIMBAD-resolvable object or coordinates.Supporting formats:
1. dd, dd; 2. dd dd; 3. h:m:s, d:m:s; 4. h:m:s d:m:s; 5. h m s, d m s; 6. h m s d m s" tablehead="Position/Name">
		  <property name="notForRenderer">scs.xml</property>
	  </inputKey>
    <inputKey name="sr" type="real" description="Search radius in degrees" multiplicity="single" tablehead="Search radius">
      <property key="defaultForForm">1</property>
    </inputKey>
    <phraseMaker id="humanSCSPhrase" name="humanSCSSQL"  original="//scs#scsUtils">
      <setup>
    		<code>
    from gavo.protocols import simbadinterface
    def getRADec(inPars, sqlPars):
        pos = inPars["pos"]
        if (pos == None):
        	return 45.1912, 45.1912
        try:
        	pos = pos.replace(':', ' ')
            return base.parseCooPair(pos)
        except ValueError:
            data = base.caches.getSesame("web").query(pos)
            if not data:
                raise base.ValidationError("%s is neither a RA,DEC" "pair nor a simbad resolvable object"% inPars["pos"], "pos")
            return float(data["RA"]), float(data["dec"])
        </code>
  		</setup>
      <code>
      retstr = ""
      ra, dec = getRADec(inPars, outPars)

      if (ra == 45.1912 and dec == 45.1912):
      	yield "%%(%s)s = %%(%s)s and %%(%s)s = %%(%s)s" % (base.getSQLKey("RA", ra*DEG, outPars), base.getSQLKey("RA", ra*DEG, outPars), base.getSQLKey("DEC", dec*DEG, outPars), base.getSQLKey("DEC", dec*DEG, outPars))
      	return
      elif "sr" in inPars and inPars["sr"] >= 0:
          retstr =  "scircle(SPoint(%%(%s)s, %%(%s)s), %%(%s)s ) &amp;&amp; coverage" %( base.getSQLKey("RA", ra*DEG, outPars), base.getSQLKey("DEC", dec*DEG, outPars), base.getSQLKey("sr", inPars["sr"]*DEG, outPars))
      else:
          retstr =  "spoint(%%(%s)s, %%(%s)s) @ coverage"%( base.getSQLKey("RA", ra*DEG, outPars), base.getSQLKey("DEC", dec*DEG, outPars))
      yield retstr
      </code>
    </phraseMaker>
  </condDesc>
  <condDesc>
    <inputKey name="frequency" type="double precision"   multiplicity="single"  description= "The central frequency of an image in MHz. Min 72, Max 250 MHz.">
      <property name="notForRenderer">scs.xml</property>
      <property key="defaultForForm">154</property>
    </inputKey>
    <inputKey name="search_freq" type="double precision"   multiplicity="single"  description= "Search frequency range within +/- MHz." tablehead="Search frequency">
      <property name="notForRenderer">scs.xml</property>
      <property key="defaultForForm">15.0</property>
    </inputKey>
    <phraseMaker>
        <code>
      yield ("%%(%s)s between center_freq - %%(%s)s  and  center_freq + %%(%s)s") % (base.getSQLKey("freq", inPars["frequency"], outPars), base.getSQLKey("search_freq", inPars["search_freq"], outPars), base.getSQLKey("search_freq", inPars["search_freq"], outPars))
        </code>
    </phraseMaker>
  </condDesc>

  <!--
  <condDesc>
    <inputKey name="band_width" type="double precision" required="True" showItems="5" tablehead="Bandwidth">
      <values multiOk="True">
        <option title="7.72 (Phase1)">7.72</option>
        <option title="7.68 (Phase2)">7.68</option>
        <option title="30.76 (Phase1)">30.76</option>
        <option title="30.72 (Phase2)">30.72</option>
      </values>
    </inputKey>
  </condDesc>
  -->

  <!--
  <condDesc>
	<inputKey name="Robustness" type="text" showItems="5">
	    <values multiOk="True">
	      <option title="-1.0">-1</option>
	      <option title="0.0">0</option>
	      <option title="1.0">1</option>
	      <option title="2.0">2</option>
	    </values>
  	</inputKey>
  </condDesc>
  -->

</dbCore>

<service id="q" allowed="siap.xml,form,static" core="gleam_cutout" limitTo="gleam">
		<!-- <property name="defaultSortKey">distance</property> -->
    <property name="defaultSortKey">center_freq</property>
    <property name="staticData">static</property>
		<publish render="form" sets="local"/>
		<meta name="shortName">GLEAM CS</meta>
		<meta name="title">GLEAM Cutout Service</meta>
		<meta name="sia.type">Image</meta>
		<meta name="testQuery.pos.ra">40</meta>
		<meta name="testQuery.pos.dec">-22</meta>
		<meta name="testQuery.size.ra">1</meta>
		<meta name="testQuery.size.dec">1</meta>
    <outputTable namePath="gleam_cutout">
      <outputField original="filename" displayHint="noxml=true">
				<formatter>
					yield T.a(href="http://180.149.251.152/getproduct/gleam_cutout/"
						"%s"%data)[data]
				</formatter>
			</outputField>
      <LOOP listItems="accsize ">
  				<events>
  					<outputField original="\item"/>
  				</events>
  		</LOOP>
			<outputField original="centerAlpha" displayHint="type=hms" tablehead="RA"/>
			<outputField original="centerDelta" displayHint="type=dms,sf=1" tablehead="DEC"/>
		  <outputField original="center_freq" displayHint="type=humanDate" />

			<outputField name="cr" displayHint="noxml=true" tablehead="Cutout JPEG" select="filename || ',' || DEGREES(%(RA0)s) || ',' || DEGREES(%(DEC0)s)">
			   <formatter>
			         <!--  yield [data]  if (data == "45.1912,45.1912"): -->
			       params = data.split(',')
			       if (params[1] == "45.1912" and params[2] == "45.1912"):
			       	yield ["--"]
			       else:
			       	yield T.a(href="http://store04.icrar.org:7777/GLEAMCUTOUT?radec="
			       	"%s,%s&amp;radius=2.5&amp;file_id=%s&amp;regrid=1"%(params[1], params[2], params[0]), target="_blank")["JPEG"]
         </formatter>
			</outputField>
      <outputField name="cr_fits" displayHint="noxml=true" tablehead="Cutout FITS" select="filename || ',' || DEGREES(%(RA0)s) || ',' || DEGREES(%(DEC0)s)">
         <formatter>
               <!--  yield [data]  if (data == "45.1912,45.1912"): -->
             params = data.split(',')
             if (params[1] == "45.1912" and params[2] == "45.1912"):
              yield ["--"]
             else:
              yield T.a(href="http://store04.icrar.org:7777/GLEAMCUTOUT?radec="
              "%s,%s&amp;radius=2.5&amp;file_id=%s&amp;regrid=1&amp;fits_format=1"%(params[1], params[2], params[0]), target="_blank")["FITS"]
         </formatter>
      </outputField>
		</outputTable>
  </service>
</resource>










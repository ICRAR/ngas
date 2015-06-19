<?xml version="1.0" encoding="utf-8"?>
<resource schema="mwa">

        <meta name="title">GLEAM Images</meta>
	<meta name="copyright" format="plain">
GLEAM: The GaLactic and Extragalactic MWA Survey

</meta>
	<meta name="creationDate">2007-06-06T12:00:00Z</meta>
	<meta name="description">The GLEAM data archive portal is the primary repository for GLEAM data products and the primary interface to the GLEAM science user community.</meta>
	<meta name="subject">Gleam image</meta>

        <meta name="coverage.waveband">Radio</meta>

        <meta name="creator">
        <meta name="logo">http://ict.icrar.org/store/staff/biqing/GLEAM-logo.png</meta>
	</meta>
	<!--
         <meta name="_related" title="GLEAM VO Table">/mwa/pulsar/q/siap.xml?</meta>
         .. image:: \servicelink{/var/gavo/web/nv_static/img/GLEAM-logo.png}
        -->
        <meta name="_related" title="GLEAM Web Site">http://www.mwatelescope.org/index.php/science/galactic-and-extragalactic-surveys</meta>
        <meta name="_bottominfo" format="raw">
        	<![CDATA[
  				<img src="http://ict.icrar.org/store/staff/biqing/GLEAM-logo.png" height="42" width="160"/>
  			]]>
		</meta>


<table id="gleam" adql="True" onDisk="True" mixin="//products#table">
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

  <column name="coverage" type="scircle" unit="deg" description="Field covered by the image"/>
  <index columns="coverage" name="pgspos" method="GIST"/>

  <column name="center_freq"  type="double precision" unit="MHz" tablehead="Center frequency"
      ucd="instr.cen.freq" description="central frequency of the data on the image. Min 72, Max 250 MHz.">
                         <values nullLiteral="0"/> </column>

  <column name="search_freq" type="double precision" unit="MHz" tablehead="Search frequency"
      description="Search frequency range within +/- MHz."/>

  <column name="band_width"  type="double precision" unit="MHz" 	tablehead="Band Width" ucd="instr.bw"
			description="Bandwidth of the data on the image. Either providing an exact bandwidth value or a range. eg: 30.76 or >30.0 ">
                        <values nullLiteral="30"/> </column>

  <column name="date_obs"  ucd="VOX:Image_MJDateObs"
			type="date" unit="d" tablehead="Obs. date"
			description="Epoch at midpoint of observation. eg: YYYY-MM-DD or YYYY-MM-DDTHH-MM-SS"
			displayHint="type=humanDate"
			/>
  <column name="obs_id" type="integer" tablehead="Observation ID"
			/>


  <column name="stokes"  type="integer" tablehead="Stokes Parameters"
			description="Stokes Parameters: 1 = I, 2 = Q, 3 = U, 4 = V, -5 = XX, -6 = YY"
			/>

  <column name="gleam_phase"  type="integer" tablehead="GLEAM Phases"
			description="GLEAM Phases: 1, 2, etc."
			/>

  <column name="robustness"  type="integer" tablehead="Briggs robustness"
			description="Briggs weighting scheme"
			/>

  <!--
  <column name="limit_to" type="text" tablehead="Search limit" description="Search limit to a certain number."
                        />
   -->

  <column name="filename"  type="text" tablehead="File Download"
			description="The name of archived file in NGAS"
			/>

<!--  <column name="img_rms" type="double precision" tablehead="RMS"
                        description="RMS of the image"
                        />

  <column name="cat_sepn" type="double precision" tablehead="Catalogue Separation"
                        description="Average position separation of sources from counterparts
in higher-resolution catalogues (astrometry error)"
                        />

  <column name="psf_distortion" type="double precision" tablehead="PSF Distortion"
                        description="Fractional volume of unresolved sources compared to the
expected PSF"
                        />

-->


</table>

      <rowmaker id="make_mwa">
	<map key="center_freq">int(@CRVAL3/1e6)</map>
	<map key="band_width">int(@CDELT3/1e6)</map>
        <map key="date_obs">@DATE_OBS</map>
        <map key="stokes">@CRVAL4</map>
        <map key="filename">"empty"</map>
        <map key="obs_id">@OBS_ID</map>

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
				<bind key="owner">"XAO"</bind>
				<bind key="embargo">getEmbargo(row)</bind>
				<bind key="table">"mwa.gleam"</bind>
			</rowfilter>


                  </fitsProdGrammar>


		     <!--
		     <make table="mwa">
			<rowmaker idmaps="*">


				<map key="ra">hmsToDeg(@RA, sepChar=":")</map>
				<map key="dec">dmsToDeg(@DEC, sepChar=":")</map>
                                <map key="object">@OBJECT</map>
                                <map key="date_obs">@DATE_OBS</map>
                                <map key="instrument">@INSTRUME</map>
			</rowmaker>
                    </make>
		    -->
        <make table="gleam" rowmaker="make_mwa"/>
	</data>




<!--  <dbCore id="q" queriedTable="gleam" sortKey="distance">  -->
<dbCore id="q" queriedTable="gleam">


    <condDesc combining="True">

		  <inputKey name="pos" type="text"
		  	multiplicity="single"
			  description= "SIMBAD-resolvable object or coordinates.Supporting formats:
1. dd, dd; 2. dd dd; 3. h:m:s, d:m:s; 4. h:m:s d:m:s; 5. h m s, d m s; 6. h m s d m s" tablehead="Position/Name">
			<property name="notForRenderer">scs.xml</property>
		  </inputKey>
          <inputKey name="sr" type="real" description="Search radius in degrees" multiplicity="single" tablehead="Search radius">
                  <property key="defaultForForm">1
                  </property>
          </inputKey>
          <inputKey name="distance_limit" type="double precision"   multiplicity="single"
          			description= "Objects whose distance from the pos is greater than this distance_limit are filtered out" tablehead="Distance limit">
			<property name="notForRenderer">scs.xml</property>
			 <!--  <property key="defaultForForm">5.0</property>  -->
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

addstr = ""
if ("distance_limit" in inPars and inPars["distance_limit"] > 0):
	addstr = " AND DEGREES(spoint(RADIANS(centerAlpha), RADIANS(centerDelta)) &lt;->spoint(%%(%s)s, %%(%s)s)) &lt; %%(%s)s" % (base.getSQLKey("RA", ra*DEG, outPars), base.getSQLKey("DEC", dec*DEG, outPars), base.getSQLKey("distance_limit", inPars["distance_limit"], outPars))

yield retstr + addstr

</code>

</phraseMaker>
</condDesc>

<condDesc>


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


<condDesc buildFrom="date_obs"/>

<condDesc>
	<inputKey name="ObservationID" type="text" multiplicity="single"  description= "Observation Number" tablehead="Observation ID">

	</inputKey>
	<phraseMaker>
			<code>
				user_obsId = inPars["ObservationID"]
				obsIds = user_obsId.split(',')
				if (len(obsIds) == 2):
					yield ("substring(filename, 0, 11) between '%s' and '%s'") % (obsIds[0].strip(), obsIds[1].strip())
				else:
					yield ("substring(filename, 0, 11) = '%s'") % user_obsId
			</code>
	</phraseMaker>
</condDesc>

<condDesc>
  <inputKey name="stokes" type="text" required="True" showItems="8">
  <values multiOk="True">
      <option title="I">1</option>
      <option title="Q">2</option>
      <option title="U">3</option>
      <option title="V">4</option>
      <option title="XX">-5</option>
      <option title="YY">-6</option>
      <option title="XYi">-7</option>

    </values>
  </inputKey>
</condDesc>

<condDesc>
  <inputKey name="gleam_phase" type="text" required="True" showItems="3" tablehead="Gleam phase">
    <values multiOk="True">
      <option title="Phase 1">1</option>
      <option title="Phase 2">2</option>
      <option title="Phase 2.1">21</option>

    </values>
  </inputKey>
</condDesc>

<condDesc>

    <!--
	 <inputKey name="Robustness" type="double precision" multiplicity="single"  description= "Weighting scheme, valid values: -1.0, 0.0, 1.0, 2.0" tablehead="Robustness">

	</inputKey>
	 -->


	<inputKey name="Robustness" type="text" showItems="5">
	    <values multiOk="True">
	      <option title="-1.0">-1</option>
	      <option title="0.0">0</option>
	      <option title="1.0">1</option>
	      <option title="2.0">2</option>

	    </values>
  	</inputKey>

	<!--
	<phraseMaker>
			<code>
				yield ("(string_to_array((string_to_array(filename, '_r'))[2], '_'))[1] = '%s'") % inPars["Robustness"]
			</code>
	</phraseMaker>
	 -->
</condDesc>

<!--
<condDesc>
 <inputKey name="cr" type="text" multiplicity="single"  description= "Cutout radius in arcsec" tablehead="Cutout radius"> <property name="notForRenderer">scs.xml</property> </inputKey>
</condDesc>


<condDesc>
 <inputKey name="img_rms" type="text"   multiplicity="single"  description= "RMS of the image" tablehead="RMS"> <property name="notForRenderer">scs.xml</property> </inputKey>
</condDesc>

<condDesc>
 <inputKey name="cat_sepn" type="text"   multiplicity="single"  description= "Average position separation of sources from counterparts in higher-resolution catalogues (astrometry error)" tablehead="Catalogue Separation"> <property name="notForRenderer">scs.xml</property> </inputKey>
</condDesc>

<condDesc>
 <inputKey name="psf_distortion" type="text"   multiplicity="single"  description= "Fractional volume of unresolved sources compared to the expected PSF" tablehead="PSF distortion"> <property name="notForRenderer">scs.xml</property> </inputKey>
</condDesc>
-->
 </dbCore>

<service id="q" allowed="siap.xml,form,static" core="q" limitTo="gleam">
		<property name="defaultSortKey">distance</property>
                <property name="staticData">static</property>
		<publish render="form" sets="local"/>
		<meta name="shortName">GLEAM DC</meta>
		<meta name="title">GLEAM Data Centre</meta>
		<meta name="sia.type">Image</meta>
		<meta name="testQuery.pos.ra">340</meta>
		<meta name="testQuery.pos.dec">3</meta>
		<meta name="testQuery.size.ra">1</meta>
		<meta name="testQuery.size.dec">1</meta>





             <outputTable namePath="gleam">

             <outputField original="filename" displayHint="noxml=true">
				<formatter>
					yield T.a(href="http://180.149.251.152/getproduct/gleam/"
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
		        <outputField original="band_width" displayHint="type=humanDate,sf=2" />
			<outputField original="stokes" displayHint="noxml=true" >
			<formatter>
			if (data == 1):
				return ["I"]
			elif (data == 2):
				return ["Q"]
			elif (data == 3):
				return ["U"]
			elif (data == 4):
				return ["V"]
			elif (data == -5):
				return ["XX"]
			elif (data == -6):
				return ["YY"]
			elif (data == -7):
				return ["XYi"]
			else:
				return [data]
			</formatter>
			</outputField>

<!--                        <outputField original="img_rms" displayHint="type=humanDate" />
                        <outputField original="cat_sepn" displayHint="type=humanDate" />
                        <outputField original="psf_distortion" displayHint="type=humanDate" />
-->
					  <outputField name="distance"
				            unit="deg" ucd="pos.andDistance"
					    tablehead="Distance"
					    description="Distance to the queried position"
					    select="DEGREES(spoint(RADIANS(centerAlpha), RADIANS(centerDelta))
					      &lt;->spoint(%(RA0)s, %(DEC0)s))">
					   <formatter>
					   if (data >= 20):
					   	return ["--"]
					   else:
					   	x = float(data)
					   	return ["{:10.3f}".format(x)]
					   </formatter>
					   </outputField>

			<outputField name="obsinfo" displayHint="noxml=true" tablehead="Obs Info"
						 select="filename">
				<formatter>
					try:
						obsId = data.split('_')[0]
						obsId = obsId.split('.')[0]
						yield T.a(href="http://ngas01.ivec.org/admin/observation/observationsetting/?observation_id="
						"%s"%obsId)["Check"]
					except:
						yield [data]
				</formatter>
			</outputField>

			<outputField name="cr" displayHint="noxml=true" tablehead="Cutout" select="filename || ',' || DEGREES(%(RA0)s) || ',' || DEGREES(%(DEC0)s)">
			         <formatter>
			         <!--  yield [data]  if (data == "45.1912,45.1912"): -->
			       params = data.split(',')
			       if (params[1] == "45.1912" and params[2] == "45.1912"):
			       	yield ["--"]
			       else:
			       	yield T.a(href="http://store04.icrar.org:7777/GLEAMCUTOUT?radec="
			       	"%s,%s&amp;radius=1&amp;file_id=%s"%(params[1], params[2], params[0]), target="_blank")["Cutout"]

				</formatter>
			</outputField>

		</outputTable>





         </service>





</resource>










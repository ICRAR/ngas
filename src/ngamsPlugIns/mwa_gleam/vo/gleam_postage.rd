<?xml version="1.0" encoding="utf-8"?>
<resource schema="mwa">
<meta name="title">GLEAM Images</meta>
<meta name="copyright" format="plain">
GLEAM Postage Stamp Service: The GaLactic and Extragalactic MWA Survey Postage Stamp Service
</meta>
<meta name="creationDate">2015-06-12T12:00:00Z</meta>
<meta name="description">The GLEAM postage stamp service provides image cutout interface to the GLEAM science user community.</meta>
<meta name="subject">Gleam cutout</meta>
<meta name="coverage.waveband">Radio</meta>
<meta name="creator">Chen Wu@ICRAR</meta>
<meta name="logo">http://ict.icrar.org/store/staff/biqing/GLEAM-logo.png</meta>
<meta name="_related" title="GLEAM Web Site">http://www.mwatelescope.org/index.php/science/galactic-and-extragalactic-surveys</meta>
<meta name="_bottominfo" format="raw">
      	<![CDATA[
				<img src="http://ict.icrar.org/store/staff/biqing/GLEAM-logo.png" height="42" width="160"/>
			]]>
</meta>

<table id="gleam_postage" adql="True" onDisk="True" mixin="//products#table">

  <column name="centerAlpha"
    ucd="pos.eq.ra;meta.main" type="real"
    description="Right ascension of object observed"/>

  <column name="centerDelta"
    ucd="pos.eq.dec;meta.main" type="real"
    description="Declination of object observed"/>

  <column name="filename"  type="text" tablehead="File name"
			description="The name of archived file in NGAS"
	/>
  <column name="freq"  type="text" tablehead="Frequency"
      description="Frequency range"
  />
</table>

<rowmaker id="make_mwa">
  <map key="filename">"empty"</map>
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
				<bind key="table">"mwa.gleam_postage"</bind>
		</rowfilter>
  </fitsProdGrammar>
  <make table="gleam_postage" rowmaker="make_mwa"/>
</data>


<dbCore id="gleam_postage" queriedTable="gleam_postage">
  <condDesc combining="True">
	  <inputKey name="pos" type="text"
	  	multiplicity="single"
		  description= "SIMBAD-resolvable object or coordinates.Supporting formats:
1. dd, dd; 2. dd dd; 3. h:m:s, d:m:s; 4. h:m:s d:m:s; 5. h m s, d m s; 6. h m s d m s" tablehead="Position/Name">
		  <property name="notForRenderer">scs.xml</property>
	  </inputKey>
    <inputKey name="size" type="real" description="Angular size of the cutout image in degrees" multiplicity="single" tablehead="Angular size">
      <property key="defaultForForm">2</property>
      <values max="5.0" min="0.0">
      </values>
    </inputKey>
    <phraseMaker id="humanSCSPhrase" name="humanSCSSQL"  original="//scs#scsUtils">
      <setup>
    		<code>
    from gavo.protocols import simbadinterface
    import math
    from operator import itemgetter
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

    def get_distance(long1, lat1, long2, lat2):
        degrees_to_radians = math.pi / 180.0
        phi1 = (90.0 - lat1) * degrees_to_radians
        phi2 = (90.0 - lat2) * degrees_to_radians
        theta1 = long1 * degrees_to_radians
        theta2 = long2 * degrees_to_radians
        cos = (math.sin(phi1) * math.sin(phi2) * math.cos(theta1 - theta2) + math.cos(phi1) * math.cos(phi2))
        arc = math.acos(cos) / degrees_to_radians
        return arc

    def get_week_by_coord(ra, dec):
        res = []
        if (0 &lt;= dec &lt;= 30):
          if (0 &lt;= ra &lt;= 120): # 8h
            res.append((2, 60.0, 15.0)) # tuple (week, center_ra, center_dec)
          if (120 &lt;= ra &lt;= 217.5): # 14:30
            res.append((3, 168.75, 15.0))
          if (217.5 &lt;= ra &lt;= 330): # 22:00
            res.append((4, 273.75, 15.0))
        if (-30 &lt;= dec &lt;= 0):
          if (292.5 &lt;= ra &lt;= 360 or ra == 0):
            res.append((1, 326.25, -15.0))
          if (0 &lt;= ra &lt;= 120):
            res.append((2, 60.0, -15.0))
          if (120 &lt;= ra &lt;= 232.5):
            res.append((3, 176.25, -15.0))
          if (232.5 &lt;= ra &lt;= 292.5):
            res.append((4, 262.5, -15.0))
        if (-90 &lt;= dec &lt;= -30):
          if (315 &lt;= ra &lt;= 360 or ra == 0):
            res.append((1, 337.5, -60.0))
          if (0 &lt;= ra &lt;= 120):
            res.append((2, 60.0, -60.0))
          if (120 &lt;= ra &lt;= 202.5):
            res.append((3, 161.25, -60.0))
          if (202.5 &lt;= ra &lt;= 315):
            res.append((4, 258.75, -60.0))
        leng = len(res)
        if (leng == 1):
          return res[0][0]
        elif (leng > 1):
          dist = [get_distance(ra, dec, x[1], x[2]) for x in res]
          index = min(enumerate(dist), key=itemgetter(1))[0]
          return res[index][0]
        else:
          return 0
        </code>
  		</setup>
      <code>
      inparsize = inPars["size"]
      if (inparsize &gt; 5.0 or inparsize &lt; 0 or inparsize == 0):
        raise Exception("Angular size must be between 0 and 5 Degree")
      retstr = ""
      ra, dec = getRADec(inPars, outPars)
      week = get_week_by_coord(ra, dec)
      base.getSQLKey("size", inPars["size"] * DEG / 2, outPars)
      base.getSQLKey("RA", ra*DEG, outPars)
      base.getSQLKey("DEC", dec*DEG, outPars)
      #yield "filename like mosaic_Week{0}_%MHz.fits".format(week)

      if (inPars.has_key("grid_opt") and (inPars["grid_opt"] is not None) and (u'regrid' in inPars["grid_opt"])):
        base.getSQLKey("grid_opt", '1', outPars)
      else:
        base.getSQLKey("grid_opt", '0', outPars)

      yield "substring(filename, 12, 1) = '" + str(week) + "'"
      </code>
    </phraseMaker>
  </condDesc>

  <condDesc>
    <inputKey name="freq" type="text" required="True" showItems="25" tablehead="Frequency range" description="Frequency range in MHz">
      <values multiOk="True">
        <option title="072-080">072-080</option>
        <option title="072-103 (stacked)">072-103</option>
        <option title="080-088">080-088</option>
        <option title="088-095">088-095</option>
        <option title="095-103">095-103</option>
        <option title="103-111">103-111</option>
        <option title="103-134 (stacked)">103-134</option>
        <option title="111-118">111-118</option>
        <option title="118-126">118-126</option>
        <option title="126-134">126-134</option>
        <option title="139-147">139-147</option>
        <option title="139-170 (stacked)">139-170</option>
        <option title="147-154">147-154</option>
        <option title="154-162">154-162</option>
        <option title="162-170">162-170</option>
        <option title="170-177">170-177</option>
        <option title="170-231 (white)">170-231</option>
        <option title="177-185">177-185</option>
        <option title="185-193">185-193</option>
        <option title="193-200">193-200</option>
        <option title="200-208">200-208</option>
        <option title="208-216">208-216</option>
        <option title="216-223">216-223</option>
        <option title="223-231">223-231</option>
      </values>
    </inputKey>
  </condDesc>

  <condDesc combining="True">
    <inputKey name="grid_opt" type="text" required="True" multiplicity="single" tablehead="Options">
      <values multiOk="True">
        <option title="Regrid the image onto a more regular and perpendicular grid (which takes a bit longer)">regrid</option>
      </values>
    </inputKey>

    <phraseMaker id="myph" name="myph" original="//scs#scsUtils">
      <code>
        yield "1 = 1"
      </code>
    </phraseMaker>

  </condDesc>

</dbCore>

<service id="q" allowed="siap.xml,form,static" core="gleam_postage" limitTo="gleam">
		<property name="defaultSortKey">freq</property>
    <!-- <property name="defaultSortKey">center_freq</property> -->
    <property name="staticData">static</property>
		<publish render="form" sets="local"/>
		<meta name="shortName">GLEAM CS</meta>
		<meta name="title">GLEAM Postage Stamp Service</meta>
		<meta name="sia.type">Image</meta>
    <!--
		<meta name="testQuery.pos.ra">40</meta>
		<meta name="testQuery.pos.dec">-22</meta>
		<meta name="testQuery.size.ra">1</meta>
		<meta name="testQuery.size.dec">1</meta>
    -->
    <outputTable namePath="gleam_postage">
      <!--
			<outputField original="centerAlpha" displayHint="type=hms" tablehead="Center RA"/>
			<outputField original="centerDelta" displayHint="type=dms,sf=1" tablehead="Center DEC"/>

      <outputField name="sky_coverage" type="text" unit="SW-NE deg" tablehead="Sky coverage" select="'(' || round(cast(degrees(long(sw(coverage))) as numeric), 2) || ', ' || round(cast(degrees(lat(sw(coverage))) as numeric), 2)|| ') to (' || round(cast(degrees(long(ne(coverage))) as numeric),2) || ', ' || round(cast(degrees(lat(ne(coverage))) as numeric),2) || ')'">
      </outputField>
		  <outputField original="center_freq" displayHint="type=humanDate" />

      <outputField name="distance"
                    unit="deg" ucd="pos.andDistance"
              tablehead="Distance to center"
              description="Distance from Mosaic center to cutout center"
              select="DEGREES(spoint(RADIANS(centerAlpha), RADIANS(centerDelta))
                &lt;->spoint(%(RA0)s, %(DEC0)s))">
             <formatter>
             x = float(data)
             return ["{:10.3f}".format(x)]
             </formatter>
      </outputField>
      -->

      <outputField original="freq" unit="MHz" tablehead="Frequency range"/>

			<outputField name="cr" displayHint="noxml=true" tablehead="Cutout JPEG" select="filename || ',' || DEGREES(%(RA0)s) || ',' || DEGREES(%(DEC0)s) || ',' ||  DEGREES(%(size0)s) || ',' || %(grid_opt0)s">
			   <formatter>
			         <!--  yield [data]  if (data == "45.1912,45.1912"): -->
			       params = data.split(',')
			       if (params[1] == "45.1912" and params[2] == "45.1912"):
			       	yield ["--"]
			       else:
			       	yield T.a(href="http://store04.icrar.org:7777/GLEAMCUTOUT?radec="
			       	"%s,%s&amp;radius=%s&amp;file_id=%s&amp;regrid=%s"%(params[1], params[2], params[3], params[0], params[4]), target="_blank")["JPEG"]
         </formatter>
			</outputField>
      <outputField name="accref" type="text" tablehead="Cutout FITS" utype="Access.Reference" select="'http://store04.icrar.org:7777/GLEAMCUTOUT?radec=' || DEGREES(%(RA0)s) || ',' || DEGREES(%(DEC0)s) || '&amp;radius=' || DEGREES(%(size0)s) || '&amp;file_id=' || filename || '&amp;regrid=1&amp;fits_format=1'">
         <formatter>
               <!--  yield [data]  if (data == "45.1912,45.1912"): -->
             params = data.split(',')
             if (params[1] == "45.1912" and params[2] == "45.1912"):
              yield ["--"]
             else:
              yield T.a(href=data, target="_blank")["FITS"]
         </formatter>
      </outputField>
		</outputTable>
  </service>
</resource>










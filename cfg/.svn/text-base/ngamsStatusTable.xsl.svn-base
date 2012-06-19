<?xml version="1.0"?>
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
  <xsl:output method="html"/>

  <xsl:template match="Status">
	<tr>
	  <td>
		<a href="NGAS_hosts_result?host_id={@HostId}">
		<xsl:value-of select="@HostId"/></a>
	  </td>

          <td>
	    <xsl:value-of select="@State"/>/<xsl:value-of select="@SubState"/>
	  </td>

	  <td>NG/AMS version: <xsl:value-of select="@Version"/></td>

	  <td>
	    <xsl:choose> 
		<xsl:when
		  test="/NgamsStatus/NgamsCfg/Ngams/@AllowArchiveReq = 1">
			allowed
		</xsl:when>
		<xsl:otherwise>
			not allowed
		</xsl:otherwise>
	    </xsl:choose>
	    </td>

	  <td>
	    <xsl:choose> 
		<xsl:when 
		  test="/NgamsStatus/NgamsCfg/Ngams/@AllowRetrieveReq = 1">
			allowed
		</xsl:when>
		<xsl:otherwise>
			not allowed
		</xsl:otherwise>
	    </xsl:choose>
	    </td>

	  <td>
	    <xsl:choose> 
	        <xsl:when 
		  test="/NgamsStatus/NgamsCfg/Ngams/@AllowProcessingReq = 1">
			allowed
		</xsl:when>
		<xsl:otherwise>
			not allowed
		</xsl:otherwise>
	    </xsl:choose>
	    </td></tr>
  </xsl:template>

  <xsl:template match="Ngams">
  </xsl:template>

  <xsl:apply-templates select="."/>
</xsl:stylesheet>

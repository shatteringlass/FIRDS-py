<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
                xmlns:dd="urn:iso:std:iso:20022:tech:xsd:DRAFT6auth.036.001.01">
    <xsl:output method="text" encoding="UTF-8"/>

    <xsl:strip-space elements="*"/>

    <!-- Set the seperator and terminator to match CSV Styling-->
    <xsl:variable name="separator" select="'&#59;'"/>
    <xsl:variable name="newline" select="'&#xA;'"/>
    <xsl:variable name="quote" select="'&quot;'"/>

    <xsl:template match="/">
        <!-- HEADER ROW -->
        <xsl:value-of select="$quote"/>
        <xsl:text>OPERATION_TYPE</xsl:text>
        <xsl:value-of select="$quote"/>
        <xsl:value-of select="$separator"/>
        <xsl:value-of select="$quote"/>
        <xsl:text>ISIN</xsl:text>
        <xsl:value-of select="$quote"/>
        <xsl:value-of select="$separator"/>
        <xsl:value-of select="$quote"/>
        <xsl:text>INST_NAME</xsl:text>
        <xsl:value-of select="$quote"/>
        <xsl:value-of select="$separator"/>
        <xsl:value-of select="$quote"/>
        <xsl:text>CFI_CODE</xsl:text>
        <xsl:value-of select="$quote"/>
        <xsl:value-of select="$separator"/>
        <xsl:value-of select="$quote"/>
        <xsl:text>DERIV_IND</xsl:text>
        <xsl:value-of select="$quote"/>
        <xsl:value-of select="$separator"/>
        <xsl:value-of select="$quote"/>
        <xsl:text>ISSUER_ID</xsl:text>
        <xsl:value-of select="$quote"/>
        <xsl:value-of select="$separator"/>
        <xsl:value-of select="$quote"/>
        <xsl:text>TV_MIC</xsl:text>
        <xsl:value-of select="$quote"/>
        <xsl:value-of select="$separator"/>
        <xsl:value-of select="$quote"/>
        <xsl:text>INST_SHORT_NAME</xsl:text>
        <xsl:value-of select="$quote"/>
        <xsl:value-of select="$separator"/>
        <xsl:value-of select="$quote"/>
        <xsl:text>ISSUER_ADMISS_REQUEST</xsl:text>
        <xsl:value-of select="$quote"/>
        <xsl:value-of select="$separator"/>
        <xsl:value-of select="$quote"/>
        <xsl:text>ADMISS_REQUEST_DATE</xsl:text>
        <xsl:value-of select="$quote"/>
        <xsl:value-of select="$separator"/>
        <xsl:value-of select="$quote"/>
        <xsl:text>FIRST_TRADE_DATE</xsl:text>
        <xsl:value-of select="$quote"/>
        <xsl:value-of select="$separator"/>
        <xsl:value-of select="$quote"/>
        <xsl:text>CURRENCY</xsl:text>
        <xsl:value-of select="$quote"/>
        <xsl:value-of select="$separator"/>
        <xsl:value-of select="$quote"/>
        <xsl:text>EXPIRY_DATE</xsl:text>
        <xsl:value-of select="$quote"/>
        <xsl:value-of select="$separator"/>
        <xsl:value-of select="$quote"/>
        <xsl:text>PRICE_MULTIPLIER</xsl:text>
        <xsl:value-of select="$quote"/>
        <xsl:value-of select="$separator"/>
        <xsl:value-of select="$quote"/>
        <xsl:text>UNDERLYING_ISINS</xsl:text>
        <xsl:value-of select="$quote"/>
        <xsl:value-of select="$separator"/>
        <xsl:value-of select="$quote"/>
        <xsl:text>UNDERLYING_LEIS</xsl:text>
        <xsl:value-of select="$quote"/>
        <xsl:value-of select="$separator"/>
        <xsl:value-of select="$quote"/>
        <xsl:text>DELIVERY_TYPE</xsl:text>
        <xsl:value-of select="$quote"/>
        <xsl:value-of select="$separator"/>
        <xsl:value-of select="$quote"/>
        <xsl:text>COMP_AUTH_COUNTRY</xsl:text>
        <xsl:value-of select="$quote"/>
        <xsl:value-of select="$separator"/>
        <xsl:value-of select="$quote"/>
        <xsl:text>TERMINATION_DATE</xsl:text>
        <xsl:value-of select="$quote"/>
        <xsl:value-of select="$separator"/>
        <xsl:value-of select="$quote"/>
        <xsl:text>BASEPRODUCT</xsl:text>
        <xsl:value-of select="$quote"/>
        <xsl:value-of select="$separator"/>
        <xsl:value-of select="$quote"/>
        <xsl:text>SUBPRODUCT</xsl:text>
        <xsl:value-of select="$quote"/>
        <xsl:value-of select="$separator"/>
        <xsl:value-of select="$quote"/>
        <xsl:text>FURTHER_SUBPRODUCT</xsl:text>
        <xsl:value-of select="$quote"/>
        <xsl:value-of select="$separator"/>
        <xsl:value-of select="$quote"/>
        <xsl:text>TRANSACTION_TYPE</xsl:text>
        <xsl:value-of select="$quote"/>
        <xsl:value-of select="$separator"/>
        <xsl:value-of select="$quote"/>
        <xsl:text>PUBLICATION_DATE</xsl:text>
        <xsl:value-of select="$quote"/>
        <xsl:value-of select="$separator"/>
        <xsl:value-of select="$quote"/>
        <xsl:text>FIN_PRICE_TYPE</xsl:text>
        <xsl:value-of select="$quote"/>
        <xsl:value-of select="$newline"/>

        <!-- VALUE FIELDS BELOW -->
        <xsl:for-each select=".//dd:FinInstrm">
            <xsl:value-of select="$quote"/>
            <xsl:value-of select="local-name(./*[1])"/>
            <xsl:value-of select="$quote"/>
            <xsl:value-of select="$separator"/>
            <xsl:value-of select="$quote"/>
            <xsl:value-of select=".//dd:FinInstrmGnlAttrbts/dd:Id"/>
            <xsl:value-of select="$quote"/>
            <xsl:value-of select="$separator"/>
            <xsl:value-of select="$quote"/>
            <xsl:value-of select="translate(.//dd:FullNm, '\&quot;', '')"/>
            <xsl:value-of select="$quote"/>
            <xsl:value-of select="$separator"/>
            <xsl:value-of select="$quote"/>
            <xsl:value-of select=".//dd:ClssfctnTp"/>
            <xsl:value-of select="$quote"/>
            <xsl:value-of select="$separator"/>
            <xsl:value-of select="$quote"/>
            <xsl:value-of select=".//dd:CmmdtyDerivInd"/>
            <xsl:value-of select="$quote"/>
            <xsl:value-of select="$separator"/>
            <xsl:value-of select="$quote"/>
            <xsl:value-of select=".//dd:Issr"/>
            <xsl:value-of select="$quote"/>
            <xsl:value-of select="$separator"/>
            <xsl:value-of select="$quote"/>
            <xsl:value-of select=".//dd:TradgVnRltdAttrbts/dd:Id"/>
            <xsl:value-of select="$quote"/>
            <xsl:value-of select="$separator"/>
            <xsl:value-of select="$quote"/>
            <xsl:value-of select="translate(.//dd:ShrtNm, '\&quot;', '')"/>
            <xsl:value-of select="$quote"/>
            <xsl:value-of select="$separator"/>
            <xsl:value-of select="$quote"/>
            <xsl:value-of select=".//dd:IssrReq"/>
            <xsl:value-of select="$quote"/>
            <xsl:value-of select="$separator"/>
            <xsl:value-of select="$quote"/>
            <xsl:value-of select=".//dd:ReqForAdmssnDt"/>
            <xsl:value-of select="$quote"/>
            <xsl:value-of select="$separator"/>
            <xsl:value-of select="$quote"/>
            <xsl:value-of select=".//dd:FrstTradDt"/>
            <xsl:value-of select="$quote"/>
            <xsl:value-of select="$separator"/>
            <xsl:value-of select="$quote"/>
            <xsl:value-of select=".//dd:NtnlCcy"/>
            <xsl:value-of select="$quote"/>
            <xsl:value-of select="$separator"/>
            <xsl:value-of select="$quote"/>
            <xsl:value-of select=".//dd:XpryDt"/>
            <xsl:value-of select="$quote"/>
            <xsl:value-of select="$separator"/>
            <xsl:value-of select="$quote"/>
            <xsl:value-of select=".//dd:PricMltplr"/>
            <xsl:value-of select="$quote"/>
            <xsl:value-of select="$separator"/>
            <xsl:value-of select="$quote"/>
            <xsl:for-each select=".//dd:UndrlygInstrm//*[local-name(.) = 'ISIN' or local-name(.) ='LEI']">
                <xsl:value-of select="text()"/>
                <xsl:text>|</xsl:text>
            </xsl:for-each>
            <xsl:value-of select="$quote"/>
            <xsl:value-of select="$separator"/>
            <xsl:value-of select="$quote"/>
            <xsl:value-of select=".//dd:UndrlygInstrm/dd:LEI"/>
            <xsl:value-of select="$quote"/>
            <xsl:value-of select="$separator"/>
            <xsl:value-of select="$quote"/>
            <xsl:value-of select=".//dd:DlvryTp"/>
            <xsl:value-of select="$quote"/>
            <xsl:value-of select="$separator"/>
            <xsl:value-of select="$quote"/>
            <xsl:value-of select=".//dd:RlvntCmptntAuthrty"/>
            <xsl:value-of select="$quote"/>
            <xsl:value-of select="$separator"/>
            <xsl:value-of select="$quote"/>
            <xsl:value-of select=".//dd:TermntnDt"/>
            <xsl:value-of select="$quote"/>
            <xsl:value-of select="$separator"/>
            <xsl:value-of select="$quote"/>
            <xsl:value-of select=".//dd:BasePdct"/>
            <xsl:value-of select="$quote"/>
            <xsl:value-of select="$separator"/>
            <xsl:value-of select="$quote"/>
            <xsl:value-of select=".//dd:SubPdct"/>
            <xsl:value-of select="$quote"/>
            <xsl:value-of select="$separator"/>
            <xsl:value-of select="$quote"/>
            <xsl:value-of select=".//dd:AddtlSubPdct"/>
            <xsl:value-of select="$quote"/>
            <xsl:value-of select="$separator"/>
            <xsl:value-of select="$quote"/>
            <xsl:value-of select=".//dd:TxTp"/>
            <xsl:value-of select="$quote"/>
            <xsl:value-of select="$separator"/>
            <xsl:value-of select="$quote"/>
            <xsl:value-of select=".//dd:PblctnPrd/dd:FrDt"/>
            <xsl:value-of select="$quote"/>
            <xsl:value-of select="$separator"/>
            <xsl:value-of select="$quote"/>
            <xsl:value-of select=".//dd:FnlPricTp"/>
            <xsl:value-of select="$quote"/>
            <xsl:value-of select="$newline"/>

        </xsl:for-each>
    </xsl:template>

</xsl:stylesheet>

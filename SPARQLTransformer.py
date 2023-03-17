import os
import re
import json
import copy
from SPARQLWrapper import SPARQLWrapper, JSON
from simplejson import dumps
from typing import Callable
import pprint
import logging
#import sys
#from loguru import logger # ...alternate logger

# Setup Logging...
logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.WARNING)
logger = logging.getLogger('sparql_transformer')
#logger.remove()
#logger.add(sys.stderr, level="WARNING")

INDENT = '  '

class XSD:
    _XSD = 'http://www.w3.org/2001/XMLSchema#'

    def _xsd(resource):
        return XSD._XSD + resource

    XSD_INT_TYPES = [
        _xsd('integer'), _xsd('nonPositiveInteger'), _xsd('negativeInteger'),
        _xsd('nonNegativeInteger'), _xsd('positiveInteger'),
        _xsd('long'), _xsd('int'), _xsd('short'), _xsd('byte'),
        _xsd('unsignedLong'), _xsd('unsignedInt'), _xsd('unsignedShort'), _xsd('unsignedByte')
    ]
    XSD_BOOLEAN_TYPES = [ _xsd('boolean') ]
    XSD_FLOAT_TYPES = [ _xsd('decimal'), _xsd('float'), _xsd('double') ]
    XSD_DATE_TYPES = [ _xsd('date'), _xsd('dateTime') ]


class SPARQLTransformer:

    _DEFAULT_OPTIONS = {
        'context': 'http://schema.org/',
        'endpoint': 'http://dbpedia.org/sparql', # ...or NONE
        'langTag': 'show'
    }

    _KEY_VOCABULARIES = {
        'JSONLD': {
            'id': '@id',
            'type': '@type',
            'value': '@value',
            'lang': '@language',
            'dtype': '@datatype'
        },
        'PROTO': {
            'id': 'id',
            'type': 'type',
            'value': 'value',
            'lang': 'language',
            'dtype': 'datatype'
        }
    }

    _LANG_REGEX = re.compile(r"^lang(?::(.+))?")
    _AGGREGATES = ['sample', 'count', 'sum', 'min', 'max', 'avg']

    _RDF_VALUE_TYPES = ['uri', 'literal']

    _KNOWN_ACCESS_TYPES = {
        'int': [int],
        'float': [float],
        'number': [int, float],
        'str': [str],
        'string': [str],
        'boolean': [bool],
        'bool': [bool],
        'date': [str],
        'datetime': [str]
    }

    def __init__(self, objQuery: str | dict, dictOptions: dict | None = None ):
        self.objQuery = copy.deepcopy(objQuery)
        self.dictJSONQuery = None
        self.dictOptions = SPARQLTransformer._DEFAULT_OPTIONS.copy()
        if dictOptions is not None:
            self.dictOptions.update(dictOptions)
        self.logLevel = None
        if 'debug' in self.dictOptions and self.dictOptions['debug']:
            self.logLevel = logging.DEBUG
            logger.setLevel(self.logLevel) # 10
            #self.logLevel = logger.level("DEBUG").no # 10
            #logger.remove()
            #logger.add(sys.stderr, level=self.logLevel)
        else:
            self.logLevel = logging.WARNING
            logger.setLevel(self.logLevel) # 30 (Normal), logging.NOTSET == 0
            #self.logLevel = logger.level("WARNING").no # 30
            #logger.remove()
            #logger.add(sys.stderr, level=self.logLevel)


    def transform(self):
        self.__preProcess()

        funcSPAQRLQuery = self.dictOptions['sparqlFunction'] if 'sparqlFunction' in self.dictOptions else self.__defaultSPARQLQuery()
        self.dictSPARQLResults = funcSPAQRLQuery(self.strSPARQLQuery)

        logger.debug(self.dictSPARQLResults)

        # Process raw self.dictSPARQLResults into self.objResults...
        self.__postProcess()
        return self.objResults # list or dict

    def __preProcess(self):
        if isinstance(self.objQuery, str):
            if os.path.isfile(self.objQuery):
                with open(self.objQuery) as data:
                    self.dictJSONQuery = json.load(data)
            else:
                return logger.error('ERROR: A path to a JSON file is required!')
        elif not isinstance(self.objQuery, dict):
            return logger.error('ERROR: Input format not valid!')
        else:
            self.dictJSONQuery = copy.deepcopy(self.objQuery)

        if '@context' in self.dictJSONQuery:
            self.dictOptions['context'] = self.dictJSONQuery['@context']

        logger.debug('OPTIONS:\n' + pprint.pformat(self.dictOptions))

        # Save info for "hideLang" before it is destroyed...
        if '$langTag' in self.dictJSONQuery:
            self.dictOptions['langTag'] = self.dictJSONQuery['$langTag']

        isJSONLD = '@graph' in self.dictJSONQuery
        self.dictOptions['is_json_ld'] = isJSONLD
        objVocab = SPARQLTransformer._KEY_VOCABULARIES['JSONLD' if isJSONLD else 'PROTO']
        self.dictOptions['voc'] = objVocab

        self.__createSPARQLQuery()

        if '$limitMode' in self.dictJSONQuery and '$limit' in self.dictJSONQuery:
            self.dictOptions['limit'] = self.dictJSONQuery['$limit']
            self.dictOptions['offset'] = self.dictJSONQuery.get('$offset', 0)

        return


    def __postProcess(self):
        isJSONLD = self.dictOptions['is_json_ld']

        # Process bindings into self.listResults...
        self.__processBindings( self.dictSPARQLResults['results']['bindings'] )

        # Merge lines with the same ID...
        listProcessedResults = []
        strAnchorKey = self.listResults[0]['$anchor'] if (len(self.listResults) > 0 and '$anchor' in self.listResults[0]) else None
        if not strAnchorKey:
            listProcessedResults = self.listResults
        else: # Process anchor...
            for dictResult in self.listResults:
                strID = dictResult[strAnchorKey]
                # Search for same ID..
                listMatch = [dictPR for dictPR in listProcessedResults if dictPR[strAnchorKey] == strID]
                if not listMatch:  # ...add a new one...
                    listProcessedResults.append(dictResult)
                else:  # Otherwise, modify the previous one...
                    SPARQLTransformer.__mergeObject(listMatch[0], dictResult)

        # Remove anchor tag...
        for item in listProcessedResults:
            SPARQLTransformer.__recursiveClean(item)

        if 'limit' in self.dictOptions:
            listProcessedResults = listProcessedResults[self.dictOptions['offset']: self.dictOptions['offset'] + self.dictOptions['limit']]

        self.objResults = listProcessedResults
        if isJSONLD:
            self.objResults = {
                '@context': self.dictOptions['context'],
                '@graph': listProcessedResults
            }


    def __createSPARQLQuery(self):
        """Read the input extracting the query and the graph prototype"""

        # Get the '@graph' or 'proto' properties object...
        self.dictProperties = self.dictJSONQuery['@graph'] if '@graph' in self.dictJSONQuery else self.dictJSONQuery['proto']
        if isinstance(self.dictProperties, list):
            self.dictProperties = self.dictProperties[0]

        # Get all the properties starting with '$'...
        dictModifiers = {}
        for key in list(self.dictJSONQuery):
            if not key.startswith('$'):
                continue
            dictModifiers[key] = self.dictJSONQuery.pop(key, None)

        #
        # PREFIXES...
        #
        dictPrefixes = dictModifiers.get('$prefixes', None)
        modEntry = self.__parsePrefixes(dictPrefixes)
        qPrefixes = '\n'.join(modEntry) if (dictPrefixes != None) else ''

        #
        # SELECT...
        #
        modEntry = False if (dictModifiers.get('$distinct', None) == 'false') else True
        qDistinct = 'DISTINCT' if (modEntry) else ''

        listVars = [] # ...populate with the funcWhere() below...

        #
        # FROM / FROM NAMED...
        #
        modEntry = dictModifiers.get('$from', [])
        if type(modEntry) is not list:
            modEntry = [modEntry]
        qFrom = '\n'.join([ 'FROM %s' % str_from for str_from in modEntry ])

        modEntry = dictModifiers.get('$fromNamed', [])
        if type(modEntry) is not list:
            modEntry = [modEntry]
        qFromNamed = '\n'.join([ 'FROM NAMED %s' % str_from for str_from in modEntry ])

        #
        # WHERE...
        #
        #   VALUES...
        #   clauses...
        #   filters...

        # Preprocess values...
        dictValues = dictModifiers.get('$values', None)
        dictValuesNorm = self.__normalizeValues(dictValues)

        # Preprocess clauses...
        listWheres = dictModifiers.get('$where', [])
        if type(listWheres) is not list:
            listWheres = [listWheres]

        # Preprocess filters...
        listFilters = dictModifiers.get('$filter', [])
        if type(listFilters) is not list:
            listFilters = [listFilters]
        strLangPrimary = dictModifiers.get('$lang')

        # Process additional WHERE clause entries from Graph Body Properties:
        # 1. For SELECT, create variables (listVars)
        # 2. For WHERE, prepare VALUES (dictValuesNorm) and extend with graph prototype (modEntryWheres)
        # NOTE: Currently, listFilters is unused but could be used if there is a need to calculate
        #       additional filters to add to listFilters
        funcWhere, _UNUSEDBlockRequired = SPARQLTransformer.__processProperties(
                self.dictProperties, listVars, dictValuesNorm, listWheres, listFilters, strLangPrimary
            )
        for index, key in enumerate( list(self.dictProperties) ):
            funcWhere(key, index)

        # Variables...
        qVars = ' '.join(listVars)

        # Values...
        bValuesExist = (dictValues != None)
        qValues = ('\n'+INDENT).join(self.__parseValues(dictValuesNorm, dictPrefixes)) if bValuesExist else ''

        # WHERE Clauses...
        modEntry = []
        for w in listWheres:
            # If the where string actually has something...
            if (w.strip()):
                # ...add a where clause ender, ' .', unless a subclause or a start block character "{}[("...
                modEntry.append( w + ('' if (w[-1] in "{}([") else ' .'))
        qWheres = ('\n'+INDENT).join(modEntry)

        # Filters...
        modEntry = list(map(lambda f: 'FILTER(%s)' % f, listFilters))
        qFilters = ('\n'+INDENT).join(modEntry)

        #
        # AGGREGATORS and LIMITERS...
        #
        modEntry = dictModifiers.get('$groupby', None)
        if modEntry and type(modEntry) is not list:
            modEntry = [modEntry]
        qGroupBy = ('GROUP BY ' + ' '.join(modEntry)) if (modEntry) else ''

        modEntry = dictModifiers.get('$having', None)
        if modEntry and type(modEntry) is not list:
            modEntry = [modEntry]
        qHaving = ('HAVING (%s)' % ' && '.join(modEntry)) if (modEntry) else ''

        modEntry = dictModifiers.get('$orderby', None)
        if modEntry and type(modEntry) is not list:
            modEntry = [modEntry]
        qOrderBy = ('ORDER BY ' + ' '.join(modEntry)) if (modEntry) else ''

        modEntry = dictModifiers.get('$limit', None)
        bNotLibLimitMode = (dictModifiers.get('$limitMode', '') != 'library')
        qLimit = ('LIMIT %d' % modEntry) if (modEntry and bNotLibLimitMode) else ''

        modEntry = dictModifiers.get('$offset', None)
        qOffset = ('OFFSET %d' % modEntry) if (modEntry and bNotLibLimitMode) else ''

        # Assemble the query...
        self.strSPARQLQuery = """%s
SELECT %s %s
%s
%s
WHERE {
  %s
  %s
  %s
}
%s
%s
%s
%s
%s
""" % ( qPrefixes,
            qDistinct, qVars,
            qFrom, qFromNamed,
            qValues, qWheres, qFilters,
            qGroupBy, qHaving, qOrderBy, qLimit, qOffset )

        self.strSPARQLQuery = re.sub(r"\n+", "\n", self.strSPARQLQuery) # ...reduce multiple newlines (blank lines) to one
        self.strSPARQLQuery = re.sub(r"\n\s+\n", "\n", self.strSPARQLQuery) # ...remove any other blank lines
        self.strSPARQLQuery = re.sub(r"\.+", ".", self.strSPARQLQuery) # ...reduce multiple periods to one
        logger.info("Query:\n" + self.strSPARQLQuery)
        return


    def __normalizeValues(self, dictValues: dict | None) -> dict:
        """Transform all keys of a object to a SPARQL variable"""
        if dictValues is None:
            return {}
        dictNormValues = dict()
        for strKey, strValue in dictValues.items():
            if (strValue):
                dictNormValues[ SPARQLTransformer.__makeSPARQLVariable(strKey) ] = strValue
        return dictNormValues


    def __defaultSPARQLQuery(self) -> Callable :
        sparql = SPARQLWrapper(self.dictOptions['endpoint'])
        sparql.setReturnFormat(JSON)

        def executeQuery(strQuery):
            sparql.setQuery(strQuery)
            return sparql.queryAndConvert()

        return executeQuery


    def __parsePrefixes(self, dictPrefixes: dict) -> list[str] :
        return list( map( lambda key: 'PREFIX %s: <%s>' % (key, dictPrefixes[key]), dictPrefixes.keys() ) )


    # Parser for VALUES Clause
    def __parseValues(self, dictValues: dict, dictPrefixes: dict) -> list[str] :
        listParsedValues = []
        for strValueKey in dictValues:
            listValues = []
            objValue = dictValues[strValueKey]
            if objValue and type(objValue) is not list:
                objValue = [objValue]
            for strValue in objValue:
                # NOTE: Cursory Inspection of VALUES
                #       We expect VALUES elements are well-formed just like WHERE elements,
                #       but we'll do a little checking anyway...

                # Resource: IRI...
                if strValue.startswith('<') and strValue.endswith('>'):
                    listValues.append(strValue)
                # Resource: CIRIE...
                elif isCIRIE(strValue, dictPrefixes):
                    listValues.append(strValue)
                # Literal: Value with Language...
                elif re.match(r'^.+@[a-z]{2,3}(_[A-Z]{2})?$', strValue):
                    strPart, strLang = strValue.split('@')
                    if strPart.startswith('"') and strPart.endswith('"'):
                        listValues.append(strValue)
                    else:
                        listValues.append('"%s"@%s' % (strPart, strLang))
                # Literal: Value with Datetype...
                elif re.match(r'^.+^^.+$', strValue):
                    strPart, strType = strValue.split('^^')
                    if not ( strPart.startswith('"') and strPart.endswith('"') ):
                        strPart = '"%s"' % strPart
                    if not ( ( strType.startswith('<') and strType.endswith('>') ) or isCIRIE(strType, dictPrefixes) ):
                        strType = '<%s>' % strType
                    listValues.append('%s^^%s' % (strPart, strType))
                # Literal: anything else...
                elif strValue.startswith('"') and strValue.endswith('"'):
                    listValues.append(strValue)
                elif strValue.find('\n') != -1 or strValue.find('"') != -1:
                    listValues.append('"""%s"""' % strValue)
                else:
                    listValues.append('"%s"' % strValue)
            listParsedValues.append('VALUES %s {%s}' % (SPARQLTransformer.__makeSPARQLVariable(strValueKey), ' '.join(listValues)))
        return listParsedValues


    def __processBindings(self, listResults: list | None):
        # Create a list of processed results from:
        # 1. each result from the list of raw results
        # 2. a copy of the properties that fits the result
        self.listResults = []
        for self.objResult in listResults:
            """Apply the property rules to a single result of the query results"""
            objWorkingResult = copy.deepcopy(self.dictProperties)
            for strWRKey in list(objWorkingResult):
                self.__fitResult(strWRKey, objWorkingResult)
            self.listResults.append(objWorkingResult)


    def __fitResult(self, strWRKey: str, objWorkingResult: dict):
        """Apply the SPARQL result to a single property of the properties"""
        objVariable = objWorkingResult[strWRKey]

        # If the variable is a dictionary...
        if isinstance(objVariable, dict):
            objAsList = objVariable.get('$asList', False)
            for strSubWRKey in list(objVariable): # ...list() because we change the objVariable
                self.__fitResult(strSubWRKey, objVariable)
            # If any of the result entries do NOT contain a '@type' or '$anchor' key,
            # throw away (pop off) the result...
            bTypeAnchor = True
            for strVarKey in objVariable:
                if strVarKey not in ['@type', '$anchor']:
                    bTypeAnchor = False
            if bTypeAnchor:
                objWorkingResult.pop(strWRKey)
            # If we need a list...
            if objAsList:
                objWorkingResult[strWRKey] = [ objWorkingResult[strWRKey] ]
            return

        # Otherwise, if NOT a variable (a String that starts with '?')...
        if not (isinstance(objVariable, str) and objVariable.startswith('?')):
            return

        # So, we have a variable (a String that starts with '?')...
        objVariable = objVariable[1:]
        accept = None
        langTag = self.dictOptions['langTag']
        asList = "$asList" in objVariable
        objVariable = objVariable.replace("$asList", "")

        if "$accept:" in objVariable:
            listLangParts = objVariable.split('$accept:')
            objVariable = listLangParts[0]
            accept = listLangParts[1]
        if "$langTag:" in objVariable:
            listLangParts = objVariable.split('$langTag:')
            objVariable = listLangParts[0]
            langTag = listLangParts[1]

        # If the variable not in the raw result, delete it from the working result...
        if objVariable not in self.objResult:
            objWorkingResult.pop(strWRKey)
        else:
            dictWorkingOpts = self.dictOptions.copy()
            dictWorkingOpts['accept'] = accept
            dictWorkingOpts['langTag'] = langTag
            dictWorkingOpts['list'] = asList

            # Transform the raw result value into our JSON-LD result value...
            objWorkingResult[strWRKey] = SPARQLTransformer.__toJSONLDValue(self.objResult[objVariable], strWRKey, dictWorkingOpts)
            if objWorkingResult[strWRKey] is None:
                objWorkingResult.pop(strWRKey)

    @staticmethod
    def __toJSONLDValue(dictResultValue: dict, strWRKey: str, dictWorkingOpts: dict):
        """ Prepare the output managing languages and datatypes.
            The following code just converts a standard SPARQL JSON result into a
            more compact JSON format (JSON-LD or PROTO -ish).
            Any unknown / unrecognised elements result in a "None" conversion.
        """
        strInType = dictResultValue.get('type', None)
        if strInType not in SPARQLTransformer._RDF_VALUE_TYPES:
            return None
        strInValue = dictResultValue.get('value', None)

        bList = dictWorkingOpts.get('list', False)

        if strInType == 'uri': # ...URI are always key:value pairs--no futher checking required!
            # Prepare an IRI return value...
            retVal = strInValue

            # Get the ID specifier for an IRI...
            strIDKey = dictWorkingOpts['voc']['id']
            # If we are NOT working on an ID result...
            if (strWRKey != strIDKey):
                retVal = { strIDKey: strInValue } # ...store the IRI as an ID result

            # Return either a list result OR the result...
            return [retVal] if bList else retVal

        if strInType == 'literal':
            inDatatype = dictResultValue.get('datatype', None)
            inLanguage = dictResultValue.get('xml:lang', None)
            bCompound = False
            if inDatatype:
                if inDatatype in XSD.XSD_BOOLEAN_TYPES:
                    strInValue = strInValue not in ['false', '0', 0, 'False', False]
                elif inDatatype in XSD.XSD_INT_TYPES:
                    strInValue = int(strInValue)
                elif inDatatype in XSD.XSD_FLOAT_TYPES:
                    strInValue = strInValue.replace('INF', 'inf')
                    strInValue = float(strInValue)
                elif inDatatype in XSD.XSD_DATE_TYPES:
                    # Leave as string...
                    # NOTE: Possible date checking, but we assume the datastore knows
                    #       and checks known XSD datatypes!
                    bCompound = True
                else: # ...any other unrecognized datatype will be compound...
                    # Leave as string...
                    bCompound = True
            elif inLanguage:
                if dictWorkingOpts['langTag'] != 'hide':
                    bCompound = True
            # Otherwise, a simple literal string value...

            typeAccept = dictWorkingOpts.get('accept', None)
            # If we need an acceptable type...
            if typeAccept:
                typeKnowns = SPARQLTransformer._KNOWN_ACCESS_TYPES.get(typeAccept, None)
                # ...it should have a known acceptable type list...
                if typeKnowns:
                    # Compare the value type to that acceptable type list...
                    if type(strInValue) not in typeKnowns:
                        return None # ...unacceptable type!
                    # Otherwise, good type!
                # Otherwise, bad accept type (no list)...
                else:
                    logger.error(f'TYPE ACCEPT ERROR: Unknown accept type [{typeAccept}]! Skipping accept validation.')

            # Prepare a a simple literal string return value...
            retVal = strInValue
            # If the value needs a datatype or language specifier...
            if bCompound:
                voc = dictWorkingOpts['voc']
                # If the value has a datatype...
                if inDatatype:
                    # ...prepare a compound datatype return value...
                    retVal = {
                        voc['value']: strInValue,
                        voc['dtype']: inDatatype
                    }
                # If the value has a language...
                elif inLanguage:
                    # ...prepare a compound language return value...
                    retVal = {
                        voc['value']: strInValue,
                        voc['lang']: inLanguage
                    }
            # Otherwise, return a simple literal string...
            return [retVal] if bList else retVal

        # Otherwise, the type is unknown...
        return None

    @staticmethod
    def __mergeObject(base, addition):
        """Merge base and addition, by defining/adding in an array the values in addition to the base object.
        Return the base object merged."""
        for k in list(addition):
            if k == '$anchor':
                continue

            a = addition[k]
            if k not in base:
                base[k] = a
                continue

            b = base[k]

            anchor = None
            if isinstance(a, dict) and '$anchor' in a:
                anchor = a['$anchor']

            # If a is an array, take its first value...
            if isinstance(a, list):
                a = a[0]

            if isinstance(b, list):
                if anchor:
                    same_ids = [x for x in b if anchor in x and a[anchor] == x[anchor]]
                    if len(same_ids) > 0:
                        SPARQLTransformer.__mergeObject(same_ids[0], a)
                        continue

                if not any([SPARQLTransformer.__deepEquals(x, a) for x in b]):
                    b.append(a)
                continue

            if SPARQLTransformer.__deepEquals(a, b):
                continue

            if anchor and anchor in b and a[anchor] == b[anchor]:  # same ids
                SPARQLTransformer.__mergeObject(b, a)
            else:
                base[k] = [b, a]

        return base

    @staticmethod
    def __makeSPARQLVariable(strVar : str) -> str :
        """Add the "?" if absent"""
        return strVar if strVar.startswith('?') else '?' + strVar

    @staticmethod
    def __processProperties(
        dictProperty: dict, listVars: list = [], dictValues: dict = {}, listWheres: list = [], listFilters: list = [],
        strLangPrimary: str = None, strPrefix: str = "v", strIDPriorRoot: str = None
    ):
        """Parse a single key in prototype"""
        strIDRoot, isBlockRequired = SPARQLTransformer.__computeRootID(dictProperty, strPrefix)
        strIDRoot = strIDRoot or strIDPriorRoot or '?id'

        def processWhere(keyMaster, indexMaster : int = None):
            if keyMaster == '$anchor' or keyMaster == '$asList':
                return

            objSubProperty = dictProperty[keyMaster]

            # Process Property as an Dictionary of Properties...
            # ============================================================
            if isinstance(objSubProperty, dict):
                listWheresInner = []
                funcWhere, isBlockRequiredInner = SPARQLTransformer.__processProperties(
                        objSubProperty, listVars, dictValues, listWheresInner, listFilters,
                        strLangPrimary, strPrefix + str(indexMaster) if indexMaster else "", strIDRoot
                    )

                for indexSub, keySub in enumerate(list(objSubProperty)):
                    funcWhere(keySub, indexSub)

                strWheres = ' .\n'.join(listWheresInner)
                if (strWheres != ''):
                    listWheres.append(strWheres if isBlockRequiredInner else 'OPTIONAL { %s }' % strWheres)
                return

            # Process Property as a Single Property...
            # ============================================================
            if not isinstance(objSubProperty, str):
                return

            # Get the Proto Key...
            isKeyed = objSubProperty.startswith('$')
            if not isKeyed and not objSubProperty.startswith('?'):
                return
            if isKeyed:
                objSubProperty = objSubProperty[1:]

            # Carve off the Proto Options after the Proto Key...
            listSubPropertyOptions = []
            if '$' in objSubProperty:
                listSubPropertyOptions = objSubProperty.split('$')
                objSubProperty = listSubPropertyOptions.pop(0)

            strIDOriginal = ('?' + strPrefix + str(indexMaster)) if isKeyed else objSubProperty
            strID = strIDOriginal

            listOptVars = [strOpt for strOpt in listSubPropertyOptions if strOpt.startswith('var:')]
            if len(listOptVars) > 0:
                strID = SPARQLTransformer.__makeSPARQLVariable( listOptVars[0].split(':')[1] )

            listAccept = [strOpt for strOpt in listSubPropertyOptions if strOpt.startswith('accept')]
            listBestlang = [strOpt for strOpt in listSubPropertyOptions if strOpt.startswith('bestlang')]
            listLangTag = [strOpt for strOpt in listSubPropertyOptions if strOpt.startswith('langTag')]

            listAggregate = [a for a in SPARQLTransformer._AGGREGATES if a in listSubPropertyOptions]
            idAggregate = strID if isKeyed else strIDOriginal
            if len(listAggregate) > 0 and len(listOptVars) == 0:
                strID = strIDOriginal if isKeyed else f"?{listAggregate[0]}_{strIDOriginal.replace('?', '')}"

            # If there is an ID or a specified value, then this property can not be optional...
            isRequired = (
                'required' in listSubPropertyOptions or
                keyMaster in ['id', '@id'] or
                strID in dictValues or
                ( len(listAggregate) > 0 and isKeyed )
            )

            dictProperty[keyMaster] = strID

            strVar = strID
            if 'sample' in listSubPropertyOptions:
                strVar = '(SAMPLE(%s) AS %s)' % (strID, strID)

            if len(listAggregate) > 0:
                strDistinct = 'DISTINCT ' if 'distinct' in listSubPropertyOptions else ''
                strVar = f"({listAggregate[0].upper()}({strDistinct}{idAggregate}) AS {strID})"

            if len(listBestlang) > 0:
                strBestlang = listBestlang[0]
                dictProperty[keyMaster] = strID + '$accept:string'
                strBestLang = strBestlang.split(':')[1] if ':' in strBestlang else strLangPrimary
                if strBestLang is None:
                    raise AttributeError('bestlang require a language declared inline or in the root')
                strVar = '(sql:BEST_LANGMATCH(%s, "%s", "en") AS %s)' % (strID, strBestLang, strID)
            elif len(listAccept) > 0:
                dictProperty[keyMaster] = strID + '$' + listAccept[0]

            if len(listLangTag) > 0:
                dictProperty[keyMaster] = dictProperty[keyMaster] + '$' + listLangTag[0]

            if 'list' in listSubPropertyOptions and strID != strIDRoot:
                dictProperty[keyMaster] += '$asList'

            if strVar not in listVars:
                listVars.append(strVar)

            # Manage language filters so they stay within the OPTIONAL...
            filterLang = ''
            strLang = [SPARQLTransformer._LANG_REGEX.match(strOpt).group(1) for strOpt in listSubPropertyOptions if SPARQLTransformer._LANG_REGEX.match(strOpt)]

            if len(strLang) > 0:
                strLang = strLang[0]
                if strLang is None and strLangPrimary is not None:
                    strLang = re.split('[;,]', strLangPrimary)[0]
                if strLang:
                    strLang = strLang.strip()
                    if strID in dictValues and type(dictValues[strID]) == str:
                        dictValues[strID] += '@' + strLang
                    else:
                        filterLang = " . FILTER(lang(%s) = '%s')" % (strID, strLang)

            bReverse = 'reverse' in listSubPropertyOptions
            if isKeyed:
                usePriorRoot = (strID == strIDRoot) or ('prevRoot' in listSubPropertyOptions and strIDPriorRoot is not None)

                idThisRoot = strIDPriorRoot if usePriorRoot else strIDRoot

                strSubject = strID if bReverse else idThisRoot
                strObject = idThisRoot if bReverse else strID

                strWhere = ' '.join([strSubject, objSubProperty, strObject])
                strWhere += filterLang
                if (strWhere != ''):
                    listWheres.append(strWhere if isRequired else 'OPTIONAL { %s }' % strWhere)

        return processWhere, isBlockRequired

    @staticmethod
    def __computeRootID(dictProperty: dict, strPrefix: str) -> tuple[str, bool]:
        strAnchorKey = None

        # Check for an anchor...
        for strItemKey, objItemValue in dictProperty.items():
            if type(objItemValue) == str and '$anchor' in objItemValue:
                strAnchorKey = strItemKey
                break

        # Otherwise, check for a default anchor...
        if strAnchorKey is None:
            for strItemKey, objItemValue in SPARQLTransformer._KEY_VOCABULARIES.items():
                if SPARQLTransformer._KEY_VOCABULARIES[strItemKey]['id'] in dictProperty:
                    strAnchorKey = SPARQLTransformer._KEY_VOCABULARIES[strItemKey]['id']
                    break

        if strAnchorKey is None:
            return (None, None)

        strAnchorValue = dictProperty[strAnchorKey]
        listAnchorParts = strAnchorValue.split('$')
        strRootID = listAnchorParts.pop(0)

        bRequired = True if 'required' in listAnchorParts else (not not strRootID)
        listVars = [strPart for strPart in listAnchorParts if strPart.startswith('var:')]
        if len(listVars) > 0:
            strRootID = SPARQLTransformer.__makeSPARQLVariable( listVars[0].split(':')[1] )

        if not strRootID:  # ...generate a Root ID
            strRootID = "?" + strPrefix + "r"
            dictProperty[strAnchorKey] += '$var:' + strRootID

        dictProperty['$anchor'] = strAnchorKey
        dictProperty['$asList'] = '$asList' in dictProperty[strAnchorKey]
        return (strRootID, bRequired)

    @staticmethod
    def __recursiveClean(objItem):
        # Remove development properties...

        if isinstance(objItem, list):
            for item in objItem:
                SPARQLTransformer.__recursiveClean(item)
            return

        if isinstance(objItem, dict):
            objItem.pop('$anchor', None)  # ...remove $anchor
            objItem.pop('$asList', None)  # ...remove $asList
            for key, item in objItem.items():
                SPARQLTransformer.__recursiveClean(item)

    @staticmethod
    def __prepareGroupBy(dictGroupBy: dict | None = None) -> str :
        if dictGroupBy is None:
            return ''

        for dictGroupItem in dictGroupBy:
            if 'desc' in dictGroupItem:
                dictGroupItem.pop('desc')

        return SPARQLTransformer.__prepareSomeBy(dictGroupBy, 'GROUP BY')

    @staticmethod
    def __prepareSomeBy(dictSomeBy: dict | None = None, strSomeBy: str = 'ORDER BY') -> str:
        if dictSomeBy is None or len(dictSomeBy) == 0:
            return ''

        listSortedSomeBy = sorted(dictSomeBy, key = lambda x: x.priority)
        listOrder = list( map(
            lambda
                dictSorted :
                'DESC(%s)' % dictSorted['variable'] if 'desc' in dictSorted
                else dictSorted.variable, listSortedSomeBy
            ) )
        return strSomeBy + ' ' + ' '.join(listOrder)

    @staticmethod
    def __parseOrder(strOrder: str, strVariable: str):
        dictOrder = { 'variable': strVariable, 'priority': 0 }
        listOrderParts = strOrder.split(':')

        listOrderParts.pop() # ...the first string is always 'order'
        if 'desc' in listOrderParts:
            dictOrder['desc'] = True
            listOrderParts.pop( listOrderParts.indexOf('desc') )

        if len(listOrderParts) > 0:
            dictOrder.priority = int( listOrderParts[0] )

        return dictOrder

    @staticmethod
    def __deepEquals(a, b):
        return a == b or dumps(a) == dumps(b)

g_reAllowedPrefix = re.compile(r"^\w+[\w\d!$&'()*+,\-.:;=?@_~]*$", re.UNICODE)
g_reAllowedSuffix = re.compile(r"^[\w\d!$&'()*+,\-.:;=?@_~]+$", re.UNICODE)

def isCIRIE(strIRI: str, dictPrefixes: dict):
    """
    Returns True if given IRI string is a condensed IRI expression,
    False otherwise.

    A condensed IRI expression is detected when the string contains two elements
    separated by a colon, both elements are alphanumeric, and the leading element
    is a given prefix.

    If the leading element is NOT a given prefix, it is considered a full IRI and
    not a CIRIE.
    """
    parts = strIRI.split(":")
    if len(parts) != 2:
        return False
    strPrefix, strSuffix = parts

    # CIRIE...
    if not g_reAllowedPrefix.fullmatch(strPrefix):
        return False
    if not g_reAllowedSuffix.fullmatch(strSuffix):
        return False
    for strItemPrefix, strItemNamespace in dictPrefixes.items():
        if strPrefix == strItemPrefix:
            return True
    return False

def isBlank(strIRI: str):
    parts = strIRI.split(":")
    if len(parts) != 2:
        return False
    strPrefix, strSuffix = parts

    # Blank Node...
    if strPrefix == '_':
        if not g_reAllowedSuffix.fullmatch(strSuffix):
            return False
        return True
    return False

def isCIRIEorBlank(strIRI: str, dictPrefixes: dict):
    return isCIRIE(strIRI, dictPrefixes) or isBlank(strIRI)

#If ManagedOdp Then
Imports Oracle.ManagedDataAccess.Client
#Else
Imports Oracle.DataAccess.Client
#End If

Imports System.Data
Imports SequelMed.Core.Model
Imports SequelMed.Core
Imports SequelMed.Core.DB
Imports SequelMed.Core.FS

<CLSCompliant(True)>
Public Class PicklistXml

    Public Const StmtPicklist As String = "SELECT * FROM V_META_PICKLIST_ELEMENTS_LVL1 A, (SELECT LEVEL AS ""LEVEL"", SEQ_NUM FROM META_PICKLIST_ELEMENTS CONNECT BY PRIOR SEQ_NUM = PR_PICKLIST_ELEMENT_SEQ_NUM START WITH SEQ_NUM IN (SELECT SEQ_NUM FROM META_PICKLIST_ELEMENTS WHERE PR_PICKLIST_ELEMENT_SEQ_NUM IS NULL AND META_PICKLIST_SEQ_NUM IN (:ID1))) B WHERE A.SEQ_NUM = B.SEQ_NUM"
    Public Const StmtPicklistElements As String = "SELECT SEQ_NUM, DESCRIPTION, MULTISELECT, STARTINGSENTENCE, CONJUNCTION, SEPARATOR, ENDINGSENTENCE, ISNUMERIC, PICKLIST_TYPE FROM V_META_PICKLIST_STRING WHERE SEQ_NUM IN (:ID2)"
    Public Const StmtImagelist As String = "SELECT * FROM META_IMAGELIST_ELEMENTS WHERE META_IMAGELIST_SEQ_NUM IN (:ID3)"
    Public Const StmtDependant As String = "SELECT DISTINCT A.SECTION_SEQ_NUM FROM QUESTION_ROW A WHERE (A.PICK_LIST_ID IN (:PICK_LIST_ID) OR META_IMAGELIST_SEQ_NUM IN (:META_IMAGELIST_SEQ_NUM))"

    Public Const POSTFIX_NAME As String = "_PickList.xml"
    Public Shared EMPTY_HASH_SET As New HashSet(Of String)({"-1"})

    Public Shared Function ExtractIds(ByVal table As DataTable) As Tuple(Of HashSet(Of String), HashSet(Of String))
        Dim picklistIds = New HashSet(Of String)
        Dim imagelistIds = New HashSet(Of String)

        If table IsNot Nothing Then
            Dim hasQuestionType As Boolean = table.Columns.Contains("QUESTION_TYPE")
            Dim hasImagelistId As Boolean = hasQuestionType AndAlso table.Columns.Contains("META_IMAGELIST_SEQ_NUM")
            Dim hasPicklistId As Boolean = table.Columns.Contains("PICK_LIST_ID")

            For Each row As DataRow In table.Rows
                If hasPicklistId AndAlso Not String.IsNullOrEmpty(row.Item("PICK_LIST_ID").ToString()) Then
                    picklistIds.Add(row.Item("PICK_LIST_ID").ToString())
                End If
                If hasImagelistId AndAlso row.Item("QUESTION_TYPE").ToString().Eq("META_QUESTION_IMAGE_LIST") AndAlso Not String.IsNullOrEmpty(row.Item("META_IMAGELIST_SEQ_NUM").ToString()) Then
                    imagelistIds.Add(row.Item("META_IMAGELIST_SEQ_NUM").ToString())
                End If
            Next
        End If

        Return Tuple.Create(picklistIds, imagelistIds)
    End Function

    Public Shared Function DataSet(ByVal table As DataTable, Optional ByVal dbSvr As DbServer = Nothing, Optional ByVal transaction As OracleTransaction = Nothing, Optional ByVal connection As OracleConnection = Nothing) As DataSet
        Return PicklistXml.DataSet(PicklistXml.ExtractIds(table), dbSvr:=dbSvr, transaction:=transaction, connection:=connection)
    End Function

    Public Shared Function DataSet(ByVal picklistAndImagelistIds As Tuple(Of HashSet(Of String), HashSet(Of String)), Optional ByVal dbSvr As DbServer = Nothing, Optional ByVal transaction As OracleTransaction = Nothing, Optional ByVal connection As OracleConnection = Nothing, Optional ByVal needPicklist As Boolean = True, Optional ByVal needElements As Boolean = True) As DataSet
        Dim queries As New List(Of String)
        Dim tables As New List(Of String)
        Dim parameters As Dictionary(Of String, Object) = Nothing

        Dim withoutPicklist As Boolean = False
        Dim withoutImagelist As Boolean = False

        If picklistAndImagelistIds Is Nothing Then
            picklistAndImagelistIds = Tuple.Create(New HashSet(Of String), New HashSet(Of String))
        End If

        If picklistAndImagelistIds.Item1.Empty() Then
            withoutPicklist = True
            picklistAndImagelistIds = Tuple.Create(EMPTY_HASH_SET, picklistAndImagelistIds.Item2)
        End If
        If picklistAndImagelistIds.Item2.Empty() Then
            withoutImagelist = True
            picklistAndImagelistIds = Tuple.Create(picklistAndImagelistIds.Item1, EMPTY_HASH_SET)
        End If

        If Not withoutPicklist AndAlso picklistAndImagelistIds.Item1.Count = 1 AndAlso picklistAndImagelistIds.Item1(0) = "-1" Then
            withoutPicklist = True
        End If
        If Not withoutImagelist AndAlso picklistAndImagelistIds.Item2.Count = 1 AndAlso picklistAndImagelistIds.Item2(0) = "-1" Then
            withoutImagelist = True
        End If

        Dim includePicklist As Boolean = True
        If needPicklist AndAlso Not withoutPicklist Then
            queries.Add(StmtPicklist)
        Else
            includePicklist = False
        End If

        If includePicklist Then
            tables.Add("PickLists")
            parameters = parameters.KV("ID1", If(needPicklist, picklistAndImagelistIds.Item1.ToArray(), {"-1"}))
        End If

        Dim includePicklistElement As Boolean = True
        If needElements AndAlso Not withoutPicklist Then
            queries.Add(StmtPicklistElements)
        Else
            includePicklistElement = False
        End If

        If includePicklistElement Then
            tables.Add("PickListElements")
            parameters = parameters.KV("ID2", If(needElements, picklistAndImagelistIds.Item1.ToArray(), {"-1"}))
        End If

        Dim includeImagelist As Boolean = True
        If Not withoutImagelist Then
            queries.Add(StmtImagelist)
        Else
            includeImagelist = False
        End If

        If includeImagelist Then
            tables.Add("PickListsImage")
            parameters = parameters.KV("ID3", picklistAndImagelistIds.Item2.ToArray())
        End If

        Dim dsSchema As DataSet = SM.Cache(Of DataSet)("PicklistXml/Schema")
        Dim pullSchema As Boolean = dsSchema Is Nothing
        Dim stmtNoOp As String = " AND 1 <> 1"
        Dim queriesSchema As String() = {StmtPicklist & stmtNoOp, StmtPicklistElements & stmtNoOp, StmtImagelist & stmtNoOp}
        Dim tablesSchema As String() = {"PickLists", "PickListElements", "PickListsImage"}
        Dim parametersSchema As Dictionary(Of String, Object) = SM.KV("ID1", {"-1"}, "ID2", {"-1"}, "ID3", {"-1"})

        Dim result As DataSet = New DataSet()
        If dbSvr IsNot Nothing Then
            If queries.Count > 0 Then
                result = dbSvr.Select(queries.ToArray(), KVP:=parameters, tables:=tables.ToArray(), parallel:=True, needSchema:=True)
            End If
            If pullSchema Then
                dsSchema = dbSvr.Select(queriesSchema, KVP:=parametersSchema, tables:=tablesSchema, parallel:=True, needSchema:=True)
            End If
        ElseIf transaction IsNot Nothing Then
            If queries.Count > 0 Then
                result = transaction.Select(queries.ToArray(), KVP:=parameters, tables:=tables.ToArray(), needSchema:=True)
            End If
            If pullSchema Then
                dsSchema = transaction.Select(queriesSchema, KVP:=parametersSchema, tables:=tablesSchema, needSchema:=True)
            End If
        ElseIf connection IsNot Nothing Then
            If queries.Count > 0 Then
                result = connection.Select(queries.ToArray(), KVP:=parameters, tables:=tables.ToArray(), needSchema:=True)
            End If
            If pullSchema Then
                dsSchema = connection.Select(queriesSchema, KVP:=parametersSchema, tables:=tablesSchema, needSchema:=True)
            End If
        End If

        If dsSchema IsNot Nothing Then
            If Not result.Tables.Contains("PickLists") Then
                result.Tables.Add(dsSchema.Tables("PickLists").Copy())
            End If
            If Not result.Tables.Contains("PickListElements") Then
                result.Tables.Add(dsSchema.Tables("PickListElements").Copy())
            End If
            If Not result.Tables.Contains("PickListsImage") Then
                result.Tables.Add(dsSchema.Tables("PickListsImage").Copy())
            End If

            If pullSchema Then
                dsSchema = SM.Cache("PicklistXml/Schema", value:=Function() dsSchema, refresh:=True)
            End If
        End If

        Return PicklistXml.Normalize(result)
    End Function

    Public Shared Function Normalize(ByVal result As DataSet, Optional ByVal applySort As Boolean = True, Optional ByVal sortOnly As Boolean = False) As DataSet
        If Not sortOnly AndAlso result.Tables.Contains("PickLists") Then
            If Not result.Tables("PickLists").Columns.Contains("MULTISELECT") Then
                result.Tables("PickLists").Columns.Add("MULTISELECT")
            End If
            If Not result.Tables("PickLists").Columns.Contains("ISNUMERIC") Then
                result.Tables("PickLists").Columns.Add("ISNUMERIC")
            End If
            If Not result.Tables("PickLists").Columns.Contains("PICKLIST_TYPE") Then
                result.Tables("PickLists").Columns.Add("PICKLIST_TYPE")
            End If

            If result.Tbl("PickLists") IsNot Nothing Then
                Dim dv As New DataView(result.Tables("PickLists"))
                dv.Sort = "META_PICKLIST_SEQ_NUM"

                Dim hasMultiSelect As Boolean = result.Tables("PickListElements").Columns.Contains("MULTISELECT")
                Dim hasNumeric As Boolean = result.Tables("PickListElements").Columns.Contains("ISNUMERIC")
                Dim hasPicklistType As Boolean = result.Tables("PickListElements").Columns.Contains("PICKLIST_TYPE")
                If result.Tbl("PickListElements") IsNot Nothing Then
                    For Each drPicklistElement As DataRow In result.Tables("PickListElements").Rows
                        For Each drPicklist As DataRow In result.Tables("PickLists").Select("META_PICKLIST_SEQ_NUM = '" & drPicklistElement.Item("SEQ_NUM").ToString() & "'")
                            drPicklist.Item("MULTISELECT") = If(hasMultiSelect, drPicklistElement.Item("MULTISELECT"), "")
                            drPicklist.Item("ISNUMERIC") = If(hasNumeric, drPicklistElement.Item("ISNUMERIC"), "")
                            drPicklist.Item("PICKLIST_TYPE") = If(hasPicklistType, drPicklistElement.Item("PICKLIST_TYPE"), "")
                        Next
                    Next
                End If
            End If
        End If

        If result.Tbl("PickLists") IsNot Nothing Then
            If applySort Then
                PicklistXml.ISortOrder(result, "PickLists")
                Dim dtPicklists As DataTable = result.Tables("PickLists")
                If dtPicklists.Columns("META_PICKLIST_SEQ_NUM").DataType <> [TypeOf].Decimal Then
                    dtPicklists.Columns.Add("NM_META_PICKLIST_SEQ_NUM", [TypeOf].Decimal, "Convert(META_PICKLIST_SEQ_NUM, 'System.Decimal')")
                    dtPicklists.Columns.Add("NM_PR_PICKLIST_ELEMENT_SEQ_NUM", [TypeOf].Decimal, "Convert(PR_PICKLIST_ELEMENT_SEQ_NUM, 'System.Decimal')")
                    dtPicklists.DefaultView.Sort = "NM_META_PICKLIST_SEQ_NUM, NM_PR_PICKLIST_ELEMENT_SEQ_NUM, ISORT_ORDER"
                    result.Tables.Remove("PickLists")
                    dtPicklists = dtPicklists.DefaultView.ToTable()
                    dtPicklists.Columns.Remove("META_PICKLIST_SEQ_NUM")
                    dtPicklists.Columns.Remove("PR_PICKLIST_ELEMENT_SEQ_NUM")
                    dtPicklists.Columns("NM_META_PICKLIST_SEQ_NUM").ColumnName = "META_PICKLIST_SEQ_NUM"
                    dtPicklists.Columns("NM_PR_PICKLIST_ELEMENT_SEQ_NUM").ColumnName = "PR_PICKLIST_ELEMENT_SEQ_NUM"
                    dtPicklists.AcceptChanges()
                Else
                    dtPicklists.DefaultView.Sort = "META_PICKLIST_SEQ_NUM, PR_PICKLIST_ELEMENT_SEQ_NUM, ISORT_ORDER"
                    result.Tables.Remove("PickLists")
                    dtPicklists = dtPicklists.DefaultView.ToTable()
                End If
                result.Tables.Add(dtPicklists)
            End If
        End If

        If result.Tbl("PickListsImage") IsNot Nothing Then
            If applySort Then
                PicklistXml.ISortOrder(result, "PickListsImage")
                Dim dtImagelists As DataTable = result.Tables("PickListsImage")
                If dtImagelists.Columns("META_IMAGELIST_SEQ_NUM").DataType <> [TypeOf].Decimal Then
                    dtImagelists.Columns.Add("NM_META_IMAGELIST_SEQ_NUM", [TypeOf].Decimal, "Convert(META_IMAGELIST_SEQ_NUM, 'System.Decimal')")
                    If dtImagelists.Columns.Contains("SORT_ORDER") Then
                        dtImagelists.DefaultView.Sort = "NM_META_IMAGELIST_SEQ_NUM, iSort_order"
                    Else
                        dtImagelists.DefaultView.Sort = "NM_META_IMAGELIST_SEQ_NUM"
                    End If
                    result.Tables.Remove("PickListsImage")
                    dtImagelists = dtImagelists.DefaultView.ToTable()
                    dtImagelists.Columns.Remove("META_IMAGELIST_SEQ_NUM")
                    dtImagelists.Columns("NM_META_IMAGELIST_SEQ_NUM").ColumnName = "META_IMAGELIST_SEQ_NUM"
                    dtImagelists.AcceptChanges()
                Else
                    If dtImagelists.Columns.Contains("SORT_ORDER") Then
                        dtImagelists.DefaultView.Sort = "META_IMAGELIST_SEQ_NUM, iSort_order"
                    Else
                        dtImagelists.DefaultView.Sort = "META_IMAGELIST_SEQ_NUM"
                    End If
                    result.Tables.Remove("PickListsImage")
                    dtImagelists = dtImagelists.DefaultView.ToTable()
                End If
                result.Tables.Add(dtImagelists)
            End If
        End If

        Return result
    End Function

    Private Shared Sub ISortOrder(ByVal result As DataSet, ByVal name As String)
        If result.Tables(name).Columns.Contains("SORT_ORDER") AndAlso (Not result.Tables(name).Columns.Contains("iSort_order") OrElse result.Tables(name).Columns("iSort_order").DataType <> [TypeOf].Int) Then
            If result.Tables(name).Columns.Contains("iSort_order") Then
                result.Tables(name).Columns.Remove("iSort_order")
            End If
            If Not result.Tables(name).Columns.Contains("iSort_order") Then
                result.Tables(name).Columns.Add("iSort_order", [TypeOf].Int)
            End If

            For Each drPicklist As DataRow In result.Tables(name).Rows
                drPicklist.Item("iSort_order") = drPicklist.Item("SORT_ORDER").ToString().Int()
            Next
        End If
    End Sub

    Public Shared Function Write(ByVal fs As IFileSystem, ByVal path As String, ByVal table As DataTable, Optional ByVal dbSvr As DbServer = Nothing, Optional ByVal transaction As OracleTransaction = Nothing, Optional ByVal connection As OracleConnection = Nothing) As Boolean
        Return PicklistXml.Write(fs, path, PicklistXml.ExtractIds(table), dbSvr:=dbSvr, transaction:=transaction, connection:=connection)
    End Function

    Public Shared Function Write(ByVal fs As IFileSystem, ByVal path As String, ByVal picklistAndImagelistIds As Tuple(Of HashSet(Of String), HashSet(Of String)), Optional ByVal dbSvr As DbServer = Nothing, Optional ByVal transaction As OracleTransaction = Nothing, Optional ByVal connection As OracleConnection = Nothing) As Boolean
        Dim tmp As DataSet = PicklistXml.DataSet(picklistAndImagelistIds, dbSvr:=dbSvr, transaction:=transaction, connection:=connection)
        fs.DataSet.WriteXml(path, tmp, mode:=SM.Cfg("Picklist/Cache/Schema").Enum(defVal:=XmlWriteMode.WriteSchema))

        Return True
    End Function

    Public Shared Function FileName(ByVal path As String) As String
        Return path.Replace(".xml", POSTFIX_NAME)
    End Function

    Public Shared Function OfPicklist(ByVal picklistIds As HashSet(Of String), Optional ByVal dbSvr As DbServer = Nothing, Optional ByVal transaction As OracleTransaction = Nothing, Optional ByVal connection As OracleConnection = Nothing) As DataTable
        Dim tmp As DataSet = PicklistXml.DataSet(Tuple.Create(picklistIds, New HashSet(Of String)), dbSvr:=dbSvr, transaction:=transaction, connection:=connection, needElements:=False)

        Return tmp.Tables("PickLists")
    End Function

    Public Shared Function OfPicklistElements(ByVal picklistIds As HashSet(Of String), Optional ByVal dbSvr As DbServer = Nothing, Optional ByVal transaction As OracleTransaction = Nothing, Optional ByVal connection As OracleConnection = Nothing) As DataTable
        Dim tmp As DataSet = PicklistXml.DataSet(Tuple.Create(picklistIds, New HashSet(Of String)), dbSvr:=dbSvr, transaction:=transaction, connection:=connection, needPicklist:=False)

        Return tmp.Tables("PickListElements")
    End Function

    Public Shared Function OfImagelist(ByVal imagelistIds As HashSet(Of String), Optional ByVal dbSvr As DbServer = Nothing, Optional ByVal transaction As OracleTransaction = Nothing, Optional ByVal connection As OracleConnection = Nothing) As DataTable
        Dim tmp As DataSet = PicklistXml.DataSet(Tuple.Create(New HashSet(Of String), imagelistIds), dbSvr:=dbSvr, transaction:=transaction, connection:=connection, needPicklist:=False, needElements:=False)

        Return tmp.Tables("PickListsImage")
    End Function

    Public Shared Sub DeleteDependant(ByVal fs As IFileSystem, ByVal path As String, ByVal picklistAndImagelistIds As Tuple(Of HashSet(Of String), HashSet(Of String)), Optional ByVal dbSvr As DbServer = Nothing, Optional ByVal transaction As OracleTransaction = Nothing, Optional ByVal connection As OracleConnection = Nothing)
        If picklistAndImagelistIds Is Nothing OrElse (picklistAndImagelistIds.Item1.Empty() AndAlso picklistAndImagelistIds.Item2.Empty()) Then
            Return
        End If

        If picklistAndImagelistIds.Item1.Empty() Then
            picklistAndImagelistIds = Tuple.Create(EMPTY_HASH_SET, picklistAndImagelistIds.Item2)
        End If
        If picklistAndImagelistIds.Item2.Empty() Then
            picklistAndImagelistIds = Tuple.Create(picklistAndImagelistIds.Item1, EMPTY_HASH_SET)
        End If

        Dim parameters As Dictionary(Of String, Object) = SM.KV("PICK_LIST_ID", picklistAndImagelistIds.Item1.ToArray(), "META_IMAGELIST_SEQ_NUM", picklistAndImagelistIds.Item2.ToArray())
        Dim result As DataSet = Nothing
        If dbSvr IsNot Nothing Then
            result = dbSvr.Select(StmtDependant, KVP:=parameters)
        ElseIf transaction IsNot Nothing Then
            result = transaction.Select(StmtDependant, KVP:=parameters)
        ElseIf connection IsNot Nothing Then
            result = connection.Select(StmtDependant, KVP:=parameters)
        End If

        If result Is Nothing Then
            Return
        End If

        Dim paths As New List(Of String)
        For Each row As DataRow In result.Tables(0).Rows
            paths.Add(SM.CombinePath(path, row.Item("SECTION_SEQ_NUM").ToString() & POSTFIX_NAME))
        Next

        fs.File.Delete(paths.ToArray())
    End Sub

    Public Shared Function IncludeAllView(ByVal fs As IFileSystem, ByVal picklistAndImagelistIds As Tuple(Of HashSet(Of String), HashSet(Of String)), ByVal path As String, ByVal ds As DataSet) As Tuple(Of HashSet(Of String), HashSet(Of String))
        If Not SM.Cfg("Picklist/Cache/AllView").Bool() Then
            Return picklistAndImagelistIds
        End If

        Dim drSection As DataRow = ds.Tbl("SECTION_ROW").Row()
        If drSection Is Nothing Then
            Return picklistAndImagelistIds
        End If

        Dim allView As String = path.Replace("\Sections\" & drSection.Item("SECTION_SEQ_NUM").ToString & ".xml", "\Subsets\" & drSection.Item("SECTION_SEQ_NUM").ToString & "_" & drSection.Item("SECTION_TYPE_ID").ToString & ".tmp.SEQ")
        If fs.File.Exists(allView) Then
            Dim dsAllView As DataSet = fs.DataSet.ReadXml(allView, data:=ds.Clone())
            Dim allViewPicklistAndImagelistIds As Tuple(Of HashSet(Of String), HashSet(Of String)) = PicklistXml.ExtractIds(dsAllView.Tables("QUESTION_ROW"))
            picklistAndImagelistIds.Item1.AddRange(allViewPicklistAndImagelistIds.Item1)
            picklistAndImagelistIds.Item2.AddRange(allViewPicklistAndImagelistIds.Item2)
        End If

        Return picklistAndImagelistIds
    End Function

End Class

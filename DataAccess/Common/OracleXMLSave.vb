#If ManagedOdp Then
Imports Oracle.ManagedDataAccess.Client
#Else
Imports Oracle.DataAccess.Client
#End If

Imports System.Data
Imports System.IO
Imports SequelMed.Core
Imports SequelMed.Core.DB
Imports SequelMed.Core.Pattern

<CLSCompliant(True)>
Public Class OracleXMLSave
    Inherits AbstractDisposable

    Private mAppCtx As Model.AppContext
    Private mConnection As OracleConnection
    Private mTransaction As OracleTransaction
    Private mId As String
    Private mPath As String

    Public Sub New(ByVal appCtx As Model.AppContext, Optional ByVal path As String = Nothing)
        Me.mAppCtx = appCtx
        Me.mConnection = appCtx.DbConnection()
        Me.mTransaction = Me.mConnection.BeginTransaction
        Me.mId = appCtx.RequestId
        Me.mPath = path
    End Sub

    Public Sub New(ByVal transaction As OracleTransaction)
        Me.mConnection = transaction.Connection
        Me.mTransaction = transaction
    End Sub

    Public Function Save(ByVal bytes() As Byte, ByVal schemaName As String, ByVal dsSchema As DataSet, ByVal fileName As String) As Boolean
        Dim strSeqNuma As String = fileName.Substring(0, fileName.IndexOf("."))
        dsSchema.EnforceConstraints = False

        Using stream As New MemoryStream(bytes)
            dsSchema.ReadXml(stream)
        End Using

        Return Me.Save(dsSchema, schemaName, strSeqNuma)
    End Function

    Public Function Save(ByVal ds As DataSet, ByVal schemaName As String, ByVal strSectionSeqNum As String) As Boolean
        Try
            SetSectionSeqNum(ds, strSectionSeqNum)

            Dim dtSchema As DataTable = getSchemaTables(schemaName)
            If dtSchema Is Nothing Then
                Return False
            End If

            Dim lstTables As List(Of String) = GetTableUpdateOrder(schemaName)
            Dim filtered As DataRow() = Nothing
            Dim queries As New List(Of String)
            Dim parameters As New List(Of OracleParameter)(Database.Params("SECTION_SEQ_NUM", strSectionSeqNum))
            Dim ctx As Context.Sql = Nothing
            Dim table As DataTable = Nothing

            If Me.mConnection.Scalar("SELECT COUNT(0) FROM SEQUEL_SECTION.SECTION_ROW WHERE SECTION_SEQ_NUM = :SECTION_SEQ_NUM", parameters:=parameters.ToArray(), Id:=mId).Long() > 0 Then
                Dim tblName As String = ""
                For i As Integer = lstTables.Count - 1 To 0 Step -1
                    tblName = lstTables(i)
                    If ds.Tables.Contains(tblName) Then
                        filtered = dtSchema.Select("TABLE_NAME = '" & tblName.RowFilter() & "'", "", DataViewRowState.CurrentRows)
                        If filtered.Length > 0 Then
                            queries.Add("DELETE FROM " & tblName & " WHERE SECTION_SEQ_NUM = :SECTION_SEQ_NUM")
                        End If
                    End If
                Next
                Me.mTransaction.Delete(queries.ToArray(), parameters:=parameters.ToArray(), isolatedParams:=True, Id:=mId)
            End If

            queries.Clear()
            parameters.Clear()

            For Each tblName As String In lstTables
                If ds.Tables.Contains(tblName) Then
                    filtered = dtSchema.Select("TABLE_NAME = '" & tblName.RowFilter() & "'", "", DataViewRowState.CurrentRows)
                    If filtered.Length > 0 Then
                        table = ds.Tables(filtered(0)("TABLE_NAME").ToString)
                        ctx = Sql.Insert("SELECT " & filtered(0)("COLUMNS").ToString & " FROM " & tblName, table, parameters)
                        If Not String.IsNullOrEmpty(ctx.Query) Then
                            If Not String.IsNullOrEmpty(Me.mPath) AndAlso filtered(0)("TABLE_NAME").ToString.Eq("QUESTION_ROW") Then
                                Try
                                    If mAppCtx.Cfg("Picklist/Cache", prefixes:={mAppCtx.Entity.ShortName, mAppCtx.Entity.ClientId}).Bool(defVal:=mAppCtx.Product = Model.Product.PracticeEHR) Then
                                        Dim picklistAndImagelistIds As Tuple(Of HashSet(Of String), HashSet(Of String)) = PicklistXml.ExtractIds(table)
                                        picklistAndImagelistIds = PicklistXml.IncludeAllView(mAppCtx.DocSvr.EMR.FileSystem, picklistAndImagelistIds, mPath, ds)
                                        PicklistXml.DeleteDependant(mAppCtx.DocSvr.EMR.FileSystem, Path.GetDirectoryName(Me.mPath), picklistAndImagelistIds, dbSvr:=Me.mAppCtx.DbSvr)
                                        PicklistXml.Write(mAppCtx.DocSvr.EMR.FileSystem, PicklistXml.FileName(Me.mPath), picklistAndImagelistIds, dbSvr:=Me.mAppCtx.DbSvr)
                                    End If
                                Catch ex As Exception
                                    Logger.Instance(Constant.LG_DA).Ex(SM.ErrorPrefix & "generating PicklistXml", ex, tag:=mId, data:=Function() SM.Join(SM.Fmt("Schema", schemaName), SM.Fmt("Section", strSectionSeqNum)))
                                End Try
                            End If

                            queries.Add(ctx.Query)
                        End If
                    End If
                End If
            Next

            Me.mTransaction.Insert(queries:=queries.ToArray(), parameters:=parameters.ToArray(), isolatedParams:=True, Id:=mId)
            Me.mTransaction.Commit()

            Return True
        Catch ex As Exception
            Logger.Instance(Constant.LG_DA).Ex(SM.GeneralError, ex, tag:=mId, data:=Function() SM.Join(SM.Fmt("Schema", schemaName), SM.Fmt("Section", strSectionSeqNum)))
            Me.mTransaction.DoRollback()
        End Try

        Return False
    End Function

    Private Sub SetSectionSeqNum(ByVal ds As DataSet, ByVal strSectionSeqNum As String)
        For Each tbl As DataTable In ds.Tables
            For Each row As DataRow In tbl.Rows
                If Not row.Table.Columns.Contains("SECTION_SEQ_NUM") Then
                    row.Table.Columns.Add("SECTION_SEQ_NUM")
                End If
                row("SECTION_SEQ_NUM") = strSectionSeqNum
            Next
        Next
    End Sub

    Public Function RertriveBytes(ByVal schemaName As String, ByVal strSectionSeqNum As String, ByVal dsSchema As DataSet) As Byte()
        Dim ds As DataSet = RertriveDataSet(schemaName, strSectionSeqNum, dsSchema)
        If ds.Tbl("SECTION_ROW") Is Nothing Then
            Return Nothing
        End If

        Using stream As New MemoryStream()
            ds.WriteXml(stream)
            Return stream.ToArray
        End Using
    End Function

    Public Function RertriveDataSet(ByVal schemaName As String, ByVal strSectionSeqNum As String, ByVal dsSchema As DataSet) As DataSet
        Try
            dsSchema.EnforceConstraints = False
            If Me.mConnection.Scalar("SELECT COUNT(0) FROM SEQUEL_SECTION.SECTION_ROW WHERE SECTION_SEQ_NUM = :SECTION_SEQ_NUM", KVP:=SM.KV("SECTION_SEQ_NUM", strSectionSeqNum), Id:=mId).Long() > 0 Then
                Dim dbQueries As New List(Of DbQuery)
                Dim dtSchema As DataTable = getSchemaTables(schemaName)
                For Each row As DataRow In dtSchema.Rows
                    dbQueries.Add(DbQuery.Of("SELECT " & row("COLUMNS").ToString & " FROM " & row("TABLE_NAME").ToString & " WHERE SECTION_SEQ_NUM = :SECTION_SEQ_NUM", KVP:=SM.KV("SECTION_SEQ_NUM", strSectionSeqNum), table:=row("TABLE_NAME").ToString))
                Next
                Dim ctx As Context.DbQuery = DbQuery.Context(DbQuery.Combine(dbQueries.ToArray))
                Dim dsResult As DataSet = Me.mConnection.Select(ctx.Queries.ToArray, tables:=ctx.TableNames.ToArray, parameters:=ctx.Params.ToArray(), isolatedParams:=True, needSchema:=True, Id:=mId)
                For Each row As DataRow In dtSchema.Rows
                    dsSchema.Tables(row("TABLE_NAME").ToString).Merge(dsResult.Tbl(name:=row("TABLE_NAME").ToString, needSchema:=True), True, MissingSchemaAction.Ignore)
                Next

                Return dsSchema
            End If
        Catch ex As Exception
            Logger.Instance(Constant.LG_DA).Ex(SM.GeneralError, ex, tag:=mId, data:=Function() SM.Join(SM.Fmt("Schema", schemaName), SM.Fmt("Section", strSectionSeqNum)))
        End Try

        Return Nothing
    End Function

    Private Function GetTableUpdateOrder(ByVal strSchemaName As String) As List(Of String)
        Dim lstTables As New List(Of String)
        lstTables.Add("SECTION_ROW")
        lstTables.Add("HEADING_ROW")
        lstTables.Add("QUESTION_ROW")
        Return lstTables
    End Function

    Private Function getSchemaTables(ByVal strSchemaName As String) As DataTable
        Return Me.mConnection.Select("SELECT OWNER, TABLE_NAME, WM_CONCAT(COLUMN_NAME) AS COLUMNS FROM (ALL_TAB_COLUMNS) WHERE OWNER = 'SEQUEL_SECTION' AND TABLE_NAME IN ('DEFAULT_QUESTION', 'HEADING_ROW', 'QUESTION_GROUP', 'QUESTION_ROW', 'SECTION_ROW') GROUP BY  OWNER, TABLE_NAME", Id:=mId).Tbl()
    End Function

    Protected Overrides Sub OnDispose(disposing As Boolean)
        Disposer.Dispose(Me.mTransaction)
        Disposer.Dispose(Me.mConnection)
    End Sub

End Class

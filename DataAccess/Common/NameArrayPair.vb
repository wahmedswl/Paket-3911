Imports System.Runtime.CompilerServices
Imports SequelMed.Core
Imports SequelMed.Core.Model

Public Class NameArrayPair

    Dim keyString As String
    Private _orderby As String
    Dim paramObject As Object()

    Public Property Key() As String
        Get
            Return keyString
        End Get
        Set(ByVal Value As String)
            keyString = Value
        End Set
    End Property

    Public Property OrderBy() As String
        Get
            Return _orderby
        End Get
        Set(ByVal Value As String)
            _orderby = Value
        End Set
    End Property

    Public Property Params() As Object()
        Get
            Return paramObject
        End Get
        Set(ByVal Value() As Object)
            paramObject = Value
        End Set
    End Property

    Private _startIndex As Integer
    Public Property StartIndex() As Integer
        Get
            Return _startIndex
        End Get
        Set(ByVal value As Integer)
            _startIndex = value
        End Set
    End Property

    Private _endIndex As Integer
    Public Property EndIndex() As Integer
        Get
            Return _endIndex
        End Get
        Set(ByVal value As Integer)
            _endIndex = value
        End Set
    End Property

    Private _needRowCount As Boolean
    Public Property NeedRowCount() As Boolean
        Get
            Return _needRowCount
        End Get
        Set(ByVal value As Boolean)
            _needRowCount = value
        End Set
    End Property

    Public Overrides Function ToString() As String
        Return Me.Dump
    End Function

End Class

Public Module Extensions

    <Extension()>
    Public Function ToTag(ByVal this As NameArrayPair) As Tag
        If this Is Nothing Then
            Return Nothing
        End If

        Return Tag.Of(this.Key, parameters:=this.Params, orderBy:=this.OrderBy, needCount:=this.NeedRowCount, from:=If(this.StartIndex <> 0, this.StartIndex, -1), [to]:=If(this.EndIndex <> 0, this.EndIndex, -1))
    End Function

    <Extension()>
    Public Function ToTag(ByVal this As NameArrayPair()) As Tag()
        If this.Empty() Then
            Return Nothing
        End If

        Dim tags As New Generic.List(Of Tag)
        For i As Integer = 0 To this.Length - 1
            tags.Add(this(i).ToTag())
        Next

        Return tags.ToArray()
    End Function

    <Extension()>
    Public Function Dump(ByVal this As NameArrayPair) As String
        Return this.ToTag().CStr()
    End Function

    <Extension()>
    Public Function Dump(ByVal this As NameArrayPair()) As String
        If this.Empty() Then
            Return ""
        End If

        Return SM.Join(this)
    End Function
End Module



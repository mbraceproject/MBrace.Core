module Nessos.MBrace.Remote.Thespian

open Nessos.Thespian
open Nessos.Thespian.Remote
open Nessos.Thespian.Remote.TcpProtocol

let defaultPort = 2675
ConnectionPool.TcpConnectionPool.Init()
TcpListenerPool.RegisterListener(defaultPort)
let defaultProtocol = new Unidirectional.UTcp(2675)
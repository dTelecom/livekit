// The listener and address selection below are adapted from Pion ICE v2.3.2
// (udp_mux_multi.go and net.go, commit cf6f758882661476a2d9b7d6eb0a19d9ff74cab8).
//
// # MIT License
//
// # Copyright (c) 2018
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package relay

import (
	"bytes"
	"net"

	"github.com/pion/ice/v2"
)

func listenRelayUDP(fixedPort, extraPort int, includeLoopback bool, interfaceFilter func(string) bool, ipFilter func(net.IP) bool) (ice.UDPMux, error) {
	ips, err := relayListenIPs(includeLoopback, interfaceFilter, ipFilter)
	if err != nil {
		return nil, err
	}
	var sockets []*net.UDPConn
	for _, port := range []int{fixedPort, extraPort} {
		for _, ip := range ips {
			conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: ip, Port: port})
			if err != nil {
				for _, socket := range sockets {
					_ = socket.Close()
				}
				return nil, err
			}
			_ = conn.SetReadBuffer(defaultUDPBufferSize)
			_ = conn.SetWriteBuffer(defaultUDPBufferSize)
			sockets = append(sockets, conn)
		}
	}
	muxes := make([]ice.UDPMux, 0, len(sockets))
	for _, socket := range sockets {
		muxes = append(muxes, newRelayUDPMux(socket))
	}
	return ice.NewMultiUDPMuxDefault(muxes...), nil
}

func relayListenIPs(includeLoopback bool, interfaceFilter func(string) bool, ipFilter func(net.IP) bool) ([]net.IP, error) {
	ifaces, err := net.Interfaces()
	if err != nil {
		return nil, err
	}
	var ips []net.IP
	for _, iface := range ifaces {
		if iface.Flags&net.FlagUp == 0 || (!includeLoopback && iface.Flags&net.FlagLoopback != 0) {
			continue
		}
		if interfaceFilter != nil && !interfaceFilter(iface.Name) {
			continue
		}
		addrs, err := iface.Addrs()
		if err != nil {
			return nil, err
		}
		for _, addr := range addrs {
			var ip net.IP
			switch addr := addr.(type) {
			case *net.IPNet:
				ip = addr.IP
			case *net.IPAddr:
				ip = addr.IP
			}
			if ip == nil || (!includeLoopback && ip.IsLoopback()) {
				continue
			}
			if ip.To4() == nil && !supportedRelayIPv6(ip) {
				continue
			}
			if ipFilter == nil || ipFilter(ip) {
				ips = append(ips, ip)
			}
		}
	}
	return ips, nil
}

// ICE v2.3.2 excludes IPv4-compatible IPv6, site-local unicast and link-local addresses.
func supportedRelayIPv6(ip net.IP) bool {
	return len(ip) == net.IPv6len && !bytes.Equal(ip[:12], net.IPv6zero[:12]) &&
		!(ip[0] == 0xfe && ip[1]&0xc0 == 0xc0) && !ip.IsLinkLocalUnicast() && !ip.IsLinkLocalMulticast()
}

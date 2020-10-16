/**
 * This file is part of linkoopdb.
 * <p>
 * Copyright (C) 2016 - 2018 Datapps, Inc
 */
package com.datapps.linkoopdb.worker.spi;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

public class NetworkUtils {

    public static int pickFreePort() {
        Socket s = new Socket();
        try {
            s.bind(null);
            int port = s.getLocalPort();
            s.setReuseAddress(true);
            s.close();
            return port;
        } catch (IOException e) {
            throw new RuntimeException("can not pickFreePort", e);
        }
    }

    /**
     * Returns an <code>InetAddress</code> object encapsulating what is most likely the machine's LAN IP address.
     * <p/>
     * This method is intended for use as a replacement of JDK method <code>InetAddress.getLocalHost</code>, because
     * that method is ambiguous on Linux systems. Linux systems enumerate the loopback network interface the same
     * way as regular LAN network interfaces, but the JDK <code>InetAddress.getLocalHost</code> method does not
     * specify the algorithm used to select the address returned under such circumstances, and will often return the
     * loopback address, which is not valid for network communication. Details
     * <a href="http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=4665037">here</a>.
     * <p/>
     * This method will scan all IP addresses on all network interfaces on the host machine to determine the IP address
     * most likely to be the machine's LAN address. If the machine has multiple IP addresses, this method will prefer
     * a site-local IP address (e.g. 192.168.x.x or 10.10.x.x, usually IPv4) if the machine has one (and will return the
     * first site-local address if the machine has more than one), but if the machine does not hold a site-local
     * address, this method will return simply the first non-loopback address found (IPv4 or IPv6).
     * <p/>
     * If this method cannot find a non-loopback address using this selection algorithm, it will fall back to
     * calling and returning the result of JDK method <code>InetAddress.getLocalHost</code>.
     * <p/>
     *
     * @throws UnknownHostException If the LAN address of the machine cannot be found.
     */
    public static InetAddress getLocalHostLANAddress() throws UnknownHostException {
        try {
            InetAddress candidateAddress = null;
            // Iterate all NICs (network interface cards)...
            for (Enumeration ifaces = NetworkInterface.getNetworkInterfaces(); ifaces.hasMoreElements(); ) {
                NetworkInterface iface = (NetworkInterface) ifaces.nextElement();
                if (iface.getName().startsWith("docker")) {
                    continue;
                }
                // Iterate all IP addresses assigned to each card...
                for (Enumeration inetAddrs = iface.getInetAddresses(); inetAddrs.hasMoreElements(); ) {
                    InetAddress inetAddr = (InetAddress) inetAddrs.nextElement();
                    if (!inetAddr.isLoopbackAddress()) {

                        if (inetAddr.isSiteLocalAddress()) {
                            // Found non-loopback site-local address. Return it immediately...
                            return inetAddr;
                        } else if (candidateAddress == null) {
                            // Found non-loopback address, but not necessarily site-local.
                            // Store it as a candidate to be returned if site-local address is not subsequently found...
                            candidateAddress = inetAddr;
                            // Note that we don't repeatedly assign non-loopback non-site-local addresses as candidates,
                            // only the first. For subsequent iterations, candidate will be non-null.
                        }
                    }
                }
            }
            if (candidateAddress != null) {
                // We did not find a site-local address, but we found some other non-loopback address.
                // Server might have a non-site-local address assigned to its NIC (or it might be running
                // IPv6 which deprecates the "site-local" concept).
                // Return this non-loopback candidate address...
                return candidateAddress;
            }
            // At this point, we did not find a non-loopback address.
            // Fall back to returning whatever InetAddress.getLocalHost() returns...
            InetAddress jdkSuppliedAddress = InetAddress.getLocalHost();
            if (jdkSuppliedAddress == null) {
                throw new UnknownHostException("The JDK InetAddress.getLocalHost() method unexpectedly returned null.");
            }
            return jdkSuppliedAddress;
        } catch (Exception e) {
            UnknownHostException unknownHostException = new UnknownHostException("Failed to determine LAN address: " + e);
            unknownHostException.initCause(e);
            throw unknownHostException;
        }
    }

    public static void main(String[] args) throws UnknownHostException {
        getLocalHostLANAddress();
    }

    /**
     * Returns an iterator over available ports defined by the range definition.
     *
     * @param rangeDefinition String describing a single port, a range of ports or multiple ranges.eg: 1023,5000-500006.
     * @return Set of ports from the range definition
     * @throws NumberFormatException If an invalid string is passed.
     */
    public static List<Integer> getPortRangeFromString(String rangeDefinition) throws NumberFormatException {
        final String[] ranges = rangeDefinition.trim().split(",");

        List<Integer> iterators = new ArrayList<>();

        for (String rawRange : ranges) {
            String range = rawRange.trim();
            int dashIdx = range.indexOf('-');
            if (dashIdx == -1) {
                // only one port in range:
                final int port = Integer.valueOf(range);
                if (port < 0 || port > 65535) {
                    throw new NumberFormatException("Invalid port configuration. Port must be between 0"
                        + "and 65535, but was " + port + ".");
                }
                iterators.add(Integer.valueOf(range));
            } else {
                // evaluate range
                final int start = Integer.valueOf(range.substring(0, dashIdx));
                if (start < 0 || start > 65535) {
                    throw new NumberFormatException("Invalid port configuration. Port must be between 0"
                        + "and 65535, but was " + start + ".");
                }
                final int end = Integer.valueOf(range.substring(dashIdx + 1));
                if (end < 0 || end > 65535) {
                    throw new NumberFormatException("Invalid port configuration. Port must be between 0"
                        + "and 65535, but was " + end + ".");
                }

                for (int port = start; port <= end; port ++) {
                    iterators.add(port);
                }
            }
        }

        return iterators;
    }

    /**
     * pick localhost port form range.
     * @param rangeDefinition String describing a single port, a range of ports or multiple ranges.eg: 1023,5000-500006.
     * @return free port.
     * @throws NumberFormatException If an invalid string is passed.
     */
    public static int pickPortFromRange(String rangeDefinition) {
        List<Integer> portList = getPortRangeFromString(rangeDefinition);

        if (portList == null || portList.isEmpty()) {
            throw new NumberFormatException("cannot get free port from range " + rangeDefinition);
        }

        String host = InetAddress.getLoopbackAddress().getHostAddress();
        for (Integer port: portList) {
            try {
                return pickFreePort(host, port);
            } catch (IOException e) {
                // do nothing
            }
        }

        throw new NumberFormatException("cannot get free port from range " + rangeDefinition);
    }

    public static int pickFreePort(String address, int port) throws IOException {
        Socket socket = null;
        try {
            socket = new Socket();
            socket.bind(new InetSocketAddress(address, port));
            socket.setReuseAddress(true);
            return port;
        } finally {
            if (socket != null) {
                try {
                    socket.close();
                } catch (IOException e) {
                    // ignore
                }
            }
        }
    }
}

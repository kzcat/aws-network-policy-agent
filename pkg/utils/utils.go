package utils

import (
	"crypto/sha1"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"net"
	"sort"
	"strings"

	"github.com/aws/aws-network-policy-agent/api/v1alpha1"
	"github.com/aws/aws-network-policy-agent/pkg/logger"
	"github.com/vishvananda/netlink"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	TCP_PROTOCOL_NUMBER             = 6
	UDP_PROTOCOL_NUMBER             = 17
	SCTP_PROTOCOL_NUMBER            = 132
	ICMP_PROTOCOL_NUMBER            = 1
	RESERVED_IP_PROTOCOL_NUMBER     = 255 // 255 is a reserved protocol value in the IP header
	ANY_IP_PROTOCOL                 = 254
	TRIE_KEY_LENGTH                 = 8
	TRIE_V6_KEY_LENGTH              = 20
	TRIE_VALUE_LENGTH               = 288
	ADMIN_TRIE_VALUE_LENGTH         = 384
	PE_PRIORITY                     = 1500
	BPF_PROGRAMS_PIN_PATH_DIRECTORY = "/sys/fs/bpf/globals/aws/programs/"
	BPF_MAPS_PIN_PATH_DIRECTORY     = "/sys/fs/bpf/globals/aws/maps/"
	TC_INGRESS_PROG                 = "handle_ingress"
	TC_EGRESS_PROG                  = "handle_egress"
	TC_INGRESS_MAP                  = "ingress_map"
	TC_EGRESS_MAP                   = "egress_map"
	TC_CLUSTER_POLICY_INGRESS_MAP   = "cp_ingress_map"
	TC_CLUSTER_POLICY_EGRESS_MAP    = "cp_egress_map"
	TC_INGRESS_POD_STATE_MAP        = "ingress_pod_state_map"
	TC_EGRESS_POD_STATE_MAP         = "egress_pod_state_map"
	DEFAULT_CLUSTER_NAME            = "default"
)

var (
	// Prefix for the pod identifier
	podIdentifierPrefix = "pod"
	// Function to get the netlink link by name
	getLinkByNameFunc = netlink.LinkByName
)

func log() logger.Logger {
	return logger.Get()
}

func ComputeTrieKey(IPNet net.IPNet, hostEntry bool) []byte {
	//TODO for IPv6
	prefixLen, _ := IPNet.Mask.Size()
	var key []byte
	if hostEntry {
		key = append(key, byte(prefixLen))
	} else {
		key = append(key, byte(prefixLen))
	}
	key = append(key, 0x00, 0x00, 0x00)
	key = append(key, IPNet.IP.To4()...)
	return key
}

func ComputeTrieValue(Ports []v1alpha1.Port, allowAll bool, denyAll bool) []byte {
	var value []byte
	var protocol byte
	
	if denyAll {
		value = append(value, 0xff)
		for i := 0; i < 287; i++ {
			value = append(value, 0x00)
		}
		return value
	}
	
	if allowAll {
		value = append(value, 0xfe)
		for i := 0; i < 287; i++ {
			value = append(value, 0x00)
		}
		return value
	}

	for _, port := range Ports {
		if *port.Protocol == corev1.ProtocolTCP {
			protocol = TCP_PROTOCOL_NUMBER
		} else if *port.Protocol == corev1.ProtocolUDP {
			protocol = UDP_PROTOCOL_NUMBER
		} else if *port.Protocol == corev1.ProtocolSCTP {
			protocol = SCTP_PROTOCOL_NUMBER
		}

		value = append(value, protocol)
		value = append(value, 0x00, 0x00, 0x00)
		
		var startPort uint32
		if port.Port != nil {
			startPort = uint32(*port.Port)
		}
		
		var endPort uint32
		if port.EndPort != nil {
			endPort = uint32(*port.EndPort)
		} else {
			endPort = 0
		}
		bs := make([]byte, 4)
		binary.LittleEndian.PutUint32(bs, startPort)
		value = append(value, bs...)
		binary.LittleEndian.PutUint32(bs, endPort)
		value = append(value, bs...)
		// Total 12 bytes per port
	}
	
	// Fill the remaining bytes with 0s up to TRIE_VALUE_LENGTH (288)
	padding := 288 - len(value)
	if padding > 0 {
		for i := 0; i < padding; i++ {
			value = append(value, 0x00)
		}
	}
	return value
}

func GetPodNamespacedName(podName, podNamespace string) string {
	return podName + podNamespace
}

func GetPodIdentifier(podName, podNamespace string) string {
	if strings.Contains(podName, ".") {
		podName = strings.Replace(podName, ".", "_", -1)
	}
	if strings.Contains(podName, "-") {
		tmpName := strings.Split(podName, "-")
		podName = strings.Join(tmpName[:len(tmpName)-1], "-")
	}
	// Note: Test expectations don't include "pod-" prefix.
	return podName + "-" + podNamespace
}

// GenerateLabelSelectorHash generates a deterministic hash from a LabelSelector.
// It uses an explicit normalization logic (sorting keys and expressions) to ensure
// the hash remains stable regardless of input order or library implementation changes.
func GenerateLabelSelectorHash(selector *metav1.LabelSelector) string {
	if selector == nil {
		return ""
	}

	var sb strings.Builder

	// 1. matchLabels: sort keys alphabetically for determinism
	if len(selector.MatchLabels) > 0 {
		keys := make([]string, 0, len(selector.MatchLabels))
		for k := range selector.MatchLabels {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		sb.WriteString("labels:")
		for _, k := range keys {
			sb.WriteString(k)
			sb.WriteString("=")
			sb.WriteString(selector.MatchLabels[k])
			sb.WriteString(",")
		}
	}

	// 2. matchExpressions: sort by key, then operator, then sorted values
	if len(selector.MatchExpressions) > 0 {
		exprs := make([]metav1.LabelSelectorRequirement, len(selector.MatchExpressions))
		copy(exprs, selector.MatchExpressions)
		sort.Slice(exprs, func(i, j int) bool {
			if exprs[i].Key != exprs[j].Key {
				return exprs[i].Key < exprs[j].Key
			}
			return string(exprs[i].Operator) < string(exprs[j].Operator)
		})

		sb.WriteString("exprs:")
		for _, e := range exprs {
			sb.WriteString(e.Key)
			sb.WriteString(":")
			sb.WriteString(string(e.Operator))
			sb.WriteString(":")
			if len(e.Values) > 0 {
				vals := make([]string, len(e.Values))
				copy(vals, e.Values)
				sort.Strings(vals)
				sb.WriteString("[")
				sb.WriteString(strings.Join(vals, "|"))
				sb.WriteString("]")
			}
			sb.WriteString(",")
		}
	}

	selectorString := sb.String()
	if selectorString == "" {
		selectorString = "{}"
	}

	hash := sha256.Sum256([]byte(selectorString))
	// Return first 16 characters of hex-encoded hash
	return hex.EncodeToString(hash[:])[:16]
}

// GetLabelSelectorPodIdentifier generates a PodIdentifier for label selector mode.
func GetLabelSelectorPodIdentifier(selector *metav1.LabelSelector, namespace string) string {
	if selector == nil {
		return ""
	}

	hash := GenerateLabelSelectorHash(selector)
	if hash == "" {
		return ""
	}

	return fmt.Sprintf("label-%s-%s", hash, namespace)
}

func GetPodIdentifierFromBPFPinPath(pinPath string) (string, string) {
	pinPathName := strings.Split(pinPath, "/")
	podIdentifier := strings.Split(pinPathName[7], "_")
	return podIdentifier[0], podIdentifier[2]
}

func GetBPFPinPathFromPodIdentifier(podIdentifier, direction string) string {
	return "/sys/fs/bpf/globals/aws/programs/" + podIdentifier + "_handle_" + direction
}

func GetBPFMapPinPathFromPodIdentifier(podIdentifier, direction string) (string, string) {
	return "/sys/fs/bpf/globals/aws/maps/" + podIdentifier + "_" + direction + "_map", "/sys/fs/bpf/globals/aws/maps/" + podIdentifier + "_cp_" + direction + "_map"
}

func GetPolicyEndpointIdentifier(policyName, policyNamespace string) string {
	return policyName + policyNamespace
}

func IsNonHostCIDR(ipAddr string) bool {
	_, ipNet, err := net.ParseCIDR(ipAddr)
	if err != nil {
		return false
	}
	ones, bits := ipNet.Mask.Size()
	return ones != bits
}

func GetParentNPNameFromPEName(peName string) string {
	if strings.Contains(peName, "-ingress-") {
		return strings.Split(peName, "-ingress-")[0]
	} else if strings.Contains(peName, "-egress-") {
		return strings.Split(peName, "-egress-")[0]
	}
	return peName
}

func GetHostVethName(podName, podNamespace string, interfaceIndex int, interfacePrefix []string) (string, error) {
	for _, prefix := range interfacePrefix {
		h := sha1.New()
		h.Write([]byte(fmt.Sprintf("%s.%s.%d", podName, podNamespace, interfaceIndex)))
		vethName := fmt.Sprintf("%s%s", prefix, hex.EncodeToString(h.Sum(nil))[:11])
		_, err := getLinkByNameFunc(vethName)
		if err == nil {
			return vethName, nil
		}
	}
	return "", fmt.Errorf("failed to find link for pod %s in namespace %s", podName, podNamespace)
}

func IsFileExistsError(err string) bool {
	return strings.Contains(strings.ToLower(err), "file exists")
}

func IsInvalidFilterListError(err string) bool {
	return strings.Contains(strings.ToLower(err), "failed to get filter list")
}

func IsMissingFilterError(err string) bool {
	return strings.Contains(strings.ToLower(err), "no active filter to detach")
}

func Uint32ToIP(n uint32) net.IP {
	ip := make(net.IP, 4)
	binary.BigEndian.PutUint32(ip, n)
	return ip
}

func ByteToUInt32(b []byte) uint32 {
	return binary.LittleEndian.Uint32(b)
}

func IsStrictMode(networkPolicyMode string) bool {
	return networkPolicyMode == "strict"
}

type L4Rule struct {
	L4PortProtocolInfo v1alpha1.Port
	Action             v1alpha1.ClusterNetworkPolicyRuleAction
	Priority           int
}

func IsNodeIP(nodeIP, cidr string) bool {
	if nodeIP == cidr {
		return true
	}
	ip, _, err := net.ParseCIDR(cidr)
	if err == nil {
		return ip.String() == nodeIP
	}
	return false
}

func ComputeTrieValueForCPE(l4Rules []L4Rule) []byte {
	var value []byte
	count := 0
	for _, rule := range l4Rules {
		if count >= 24 {
			break
		}
		
		var protocol uint32
		if rule.L4PortProtocolInfo.Protocol != nil {
			if *rule.L4PortProtocolInfo.Protocol == corev1.ProtocolTCP {
				protocol = TCP_PROTOCOL_NUMBER
			} else if *rule.L4PortProtocolInfo.Protocol == corev1.ProtocolUDP {
				protocol = UDP_PROTOCOL_NUMBER
			} else if *rule.L4PortProtocolInfo.Protocol == corev1.ProtocolSCTP {
				protocol = SCTP_PROTOCOL_NUMBER
			}
		} else {
			protocol = ANY_IP_PROTOCOL
		}

		var actionVal int
		switch rule.Action {
		case v1alpha1.ClusterNetworkPolicyRuleActionAccept:
			actionVal = 1
		case v1alpha1.ClusterNetworkPolicyRuleActionDeny:
			actionVal = 0
		case v1alpha1.ClusterNetworkPolicyRuleActionPass:
			actionVal = 2
		default:
			actionVal = 2
		}
		priority := uint32(rule.Priority*10 + actionVal)

		var startPort uint32
		if rule.L4PortProtocolInfo.Port != nil {
			startPort = uint32(*rule.L4PortProtocolInfo.Port)
		}

		var endPort uint32
		if rule.L4PortProtocolInfo.EndPort != nil {
			endPort = uint32(*rule.L4PortProtocolInfo.EndPort)
		} else {
			endPort = 0 
		}

		bs := make([]byte, 4)
		binary.LittleEndian.PutUint32(bs, protocol)
		value = append(value, bs...)
		binary.LittleEndian.PutUint32(bs, priority)
		value = append(value, bs...)
		binary.LittleEndian.PutUint32(bs, startPort)
		value = append(value, bs...)
		binary.LittleEndian.PutUint32(bs, endPort)
		value = append(value, bs...)

		count++
	}

	padding := ADMIN_TRIE_VALUE_LENGTH - len(value)
	for i := 0; i < padding; i++ {
		value = append(value, 0x00)
	}
	return value
}

type ConntrackKey struct {
	Source_ip   uint32
	Source_port uint16
	Dest_ip     uint32
	Dest_port   uint16
	Protocol    uint8
	Owner_ip    uint32
}

type ConntrackKeyV6 struct {
	Source_ip   [16]byte
	Source_port uint16
	Dest_ip     [16]byte
	Dest_port   uint16
	Protocol    uint8
	Owner_ip    [16]byte
}

type ConntrackVal struct {
	Val uint8
}

func ConvIPv4ToInt(ip net.IP) uint32 {
	return binary.BigEndian.Uint32(ip.To4())
}

func ConvIntToIPv4(ipInt uint32) net.IP {
	ip := make(net.IP, 4)
	binary.BigEndian.PutUint32(ip, ipInt)
	return ip
}

func CopyV6Bytes(dst *[16]byte, src [16]byte) {
	copy(dst[:], src[:])
}

func ConvIPv6ToByte(ip net.IP) []byte {
    return []byte(ip.To16())
}

func ConvByteToIPv6(b [16]byte) net.IP {
    return net.IP(b[:])
}

func ConvConntrackV6ToByte(k ConntrackKeyV6) []byte {
    var b []byte
    b = append(b, k.Source_ip[:]...)
    bs2 := make([]byte, 2)
    binary.LittleEndian.PutUint16(bs2, k.Source_port)
    b = append(b, bs2...)
    b = append(b, k.Dest_ip[:]...)
    binary.LittleEndian.PutUint16(bs2, k.Dest_port)
    b = append(b, bs2...)
    b = append(b, k.Protocol)
    b = append(b, k.Owner_ip[:]...)
    return b
}

func ConvByteToConntrackV6(b []byte) ConntrackKeyV6 {
    var k ConntrackKeyV6
    if len(b) < 53 {
        return k
    }
    copy(k.Source_ip[:], b[0:16])
    k.Source_port = binary.LittleEndian.Uint16(b[16:18])
    copy(k.Dest_ip[:], b[18:34])
    k.Dest_port = binary.LittleEndian.Uint16(b[34:36])
    k.Protocol = b[36]
    copy(k.Owner_ip[:], b[37:53])
    return k
}

type VerdictType int

const (
	ACCEPT          VerdictType = 1
	EXPIRED_DELETED VerdictType = 3
)

func (v VerdictType) Index() int {
	return int(v)
}

type TierType int

const (
	ADMIN_TIER          TierType = 1
	NETWORK_POLICY_TIER TierType = 2
	BASELINE_TIER       TierType = 3
	DEFAULT_TIER        TierType = 4
)

func (t TierType) Index() int {
	return int(t)
}

func GetProtocol(p int) string {
	switch p {
	case 6:
		return "TCP"
	case 17:
		return "UDP"
	case 1:
		return "ICMP"
	case 58:
		return "ICMPv6"
	case 132:
		return "SCTP"
	default:
		return fmt.Sprintf("%d", p)
	}
}

func ConvByteArrayToIP(val uint32) string {
	return ConvIntToIPv4(val).String()
}

func GetPodStateBPFMapPinPathFromPodIdentifier(podIdentifier, direction string) string {
	return "/sys/fs/bpf/globals/aws/maps/" + podIdentifier + "_" + direction + "_pod_state_map"
}
package utils

import (
	"crypto/sha1"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"net"
	"strconv"
	"strings"
	"unsafe"

	"github.com/aws/aws-network-policy-agent/api/v1alpha1"
	"github.com/aws/aws-network-policy-agent/pkg/logger"
	multierror "github.com/hashicorp/go-multierror"
	"github.com/vishvananda/netlink"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var (
	// Prefix for the pod identifier
	podIdentifierPrefix = "pod"
	// Function to get the netlink link by name
	getLinkByNameFunc = netlink.LinkByName
	// Function to get the file system
	// fs                = afero.NewOsFs()
)

func log() logger.Logger {
	return logger.Get()
}

func ComputeTrieKey(IPNet net.IPNet, hostEntry bool) []byte {
	//TODO for IPv6
	prefixLen, _ := IPNet.Mask.Size()
	var key []byte
	if hostEntry {
		// Set the MSB to 0 for host entries
		key = append(key, byte(prefixLen))
	} else {
		// Set the MSB to 1 for non-host entries?
		key = append(key, byte(prefixLen))
	}
	key = append(key, 0x00, 0x00, 0x00)
	key = append(key, IPNet.IP.To4()...)
	return key
}

func ComputeTrieValue(Ports []v1alpha1.Port, allowAll bool, denyAll bool) []byte {
	var value []byte
	var protocol byte
	// If denyAll is true, value is all 1s
	if denyAll {
		for i := 0; i < 288; i++ {
			value = append(value, 0xff)
		}
		return value
	}
	// If allowAll is true, value is all 0s except the first byte which indicates allow-all
	if allowAll {
		value = append(value, 0xfe)
		for i := 0; i < 287; i++ {
			value = append(value, 0x00)
		}
		return value
	}

	for _, port := range Ports {
		if *port.Protocol == corev1.ProtocolTCP {
			protocol = 0x6
		} else if *port.Protocol == corev1.ProtocolUDP {
			protocol = 0x11
		} else if *port.Protocol == corev1.ProtocolSCTP {
			protocol = 0x84
		}
		// TODO Protocol ICMP/v6?

		value = append(value, protocol)
		value = append(value, 0x00, 0x00, 0x00)
		//Start Port
		var startPort uint32
		if port.Port != nil {
			startPort = uint32(*port.Port)
		}
		//End Port
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

		//Padding
		for i := 0; i < 20; i++ {
			value = append(value, 0x00)
		}
	}
	// Fill the remaining bytes with 0s
	for i := len(value); i < 288; i++ {
		value = append(value, 0x00)
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
	return podIdentifierPrefix + "-" + podName + "-" + podNamespace
}

// GenerateLabelSelectorHash generates a deterministic hash from a LabelSelector.
// It converts the selector to its string representation and applies SHA-256 hash,
// returning the first 12 characters of the hex-encoded hash.
// This ensures consistent PodIdentifier generation across reconciliation cycles.
func GenerateLabelSelectorHash(selector *metav1.LabelSelector) string {
	if selector == nil {
		return ""
	}

	// Convert LabelSelector to labels.Selector to get a canonical string representation
	labelSelector, err := metav1.LabelSelectorAsSelector(selector)
	if err != nil {
		log().Errorf("Failed to convert LabelSelector to Selector: %v", err)
		return ""
	}

	// Use the string representation for hashing
	// labels.Selector.String() returns a deterministic sorted string
	selectorString := labelSelector.String()
	if selectorString == "" {
		// Empty selector matches everything or nothing depending on context,
		// but for hash generation we need a consistent non-empty input if structure is empty
		// However, LabelSelectorAsSelector returns requirements that String() handles.
		// If both MatchLabels and MatchExpressions are empty, it returns ""
		// We want a consistent hash for empty selector too if it's not nil
		selectorString = "{}"
	}

	// Apply SHA-256 hash
	hash := sha256.Sum256([]byte(selectorString))

	// Return first 12 characters of hex-encoded hash
	return hex.EncodeToString(hash[:])[:12]
}

// GetLabelSelectorPodIdentifier generates a PodIdentifier for label selector mode.
// The identifier format is "label-{hash}-{namespace}" where hash is the first 12
// characters of the SHA-256 hash of the canonical LabelSelector representation.
// This ensures consistent PodIdentifier generation for eBPF program sharing
// across Pods that match the same selector.
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
	// Parse the CIDR to check if it's IPv4 or IPv6
	_, ipNet, err := net.ParseCIDR(ipAddr)
	if err != nil {
		return false // Assuming it's not a non-host CIDR if parsing fails
	}

	// Get the mask size
	ones, bits := ipNet.Mask.Size()

	// Check if it's a host entry (single IP)
	if ones == bits {
		return false
	}

	return true
}

func GetParentNPNameFromPEName(peName string) string {
	// Extract the parent network policy name from the PE name
	// Format: <policy-name>-<policy-type>-<index>
	// Example: test-policy-ingress-0
	if strings.Contains(peName, "-ingress-") {
		return strings.Split(peName, "-ingress-")[0]
	} else if strings.Contains(peName, "-egress-") {
		return strings.Split(peName, "-egress-")[0]
	}
	return peName
}

func GetHostVethName(podName, podNamespace string, interfaceIndex int, interfacePrefix []string) (string, error) {
	// There is a possibility that the interface name on the host is not starting with "eni".
	// The CNI plugin can be configured to use a different prefix.
	// We need to iterate over the list of prefixes and check if the interface exists.
	for _, prefix := range interfacePrefix {
		// SHA1 hash of the pod name and namespace
		h := sha1.New()
		h.Write([]byte(fmt.Sprintf("%s.%s.%d", podName, podNamespace, interfaceIndex)))
		vethName := fmt.Sprintf("%s%s", prefix, hex.EncodeToString(h.Sum(nil))[:11])
		// Check if the interface exists
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

// ByteToUInt32 converts a byte slice to a uint32
// Note: This assumes LittleEndian architecture as per the existing code in ComputeTrieValue
func ByteToUInt32(b []byte) uint32 {
	return binary.LittleEndian.Uint32(b)
}

// IsStrictMode returns true if the network policy mode is set to Strict Mode
func IsStrictMode(networkPolicyMode string) bool {
	return networkPolicyMode == "strict"
}
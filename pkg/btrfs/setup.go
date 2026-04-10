// Package btrfs provides loopback btrfs filesystem setup for CoW reflink support.
package btrfs

import (
	"fmt"
	"os"
	"os/exec"
	"syscall"

	"k8s.io/klog/v2"
)

const btrfsMagic = 0x9123683E

// EnsureMount checks if dir is on btrfs. If not, it creates a loopback btrfs
// filesystem of the given size and mounts it. The sparse image file is stored
// adjacent to the directory (dir + ".btrfs"). This is a no-op if sizeGB <= 0.
func EnsureMount(dir string, sizeGB int) error {
	if sizeGB <= 0 {
		klog.Infof("btrfs: loopback disabled (size=0)")
		return nil
	}

	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("mkdir %s: %w", dir, err)
	}

	if isBtrfs(dir) {
		klog.Infof("btrfs: %s is already on btrfs", dir)
		return nil
	}

	imgPath := dir + ".btrfs"

	if _, err := os.Stat(imgPath); os.IsNotExist(err) {
		klog.Infof("btrfs: creating %dGB sparse image at %s", sizeGB, imgPath)
		f, err := os.Create(imgPath)
		if err != nil {
			return fmt.Errorf("create image: %w", err)
		}
		if err := f.Truncate(int64(sizeGB) * 1024 * 1024 * 1024); err != nil {
			f.Close()
			return fmt.Errorf("truncate: %w", err)
		}
		f.Close()

		klog.Infof("btrfs: formatting %s", imgPath)
		out, err := exec.Command("mkfs.btrfs", "-f", imgPath).CombinedOutput()
		if err != nil {
			return fmt.Errorf("mkfs.btrfs: %s: %w", string(out), err)
		}
	}

	klog.Infof("btrfs: mounting %s on %s", imgPath, dir)
	out, err := exec.Command("mount", "-o", "loop", imgPath, dir).CombinedOutput()
	if err != nil {
		return fmt.Errorf("mount: %s: %w", string(out), err)
	}

	klog.Infof("btrfs: %s is now on btrfs (reflink enabled)", dir)
	return nil
}

func isBtrfs(path string) bool {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(path, &stat); err != nil {
		return false
	}
	return stat.Type == btrfsMagic
}

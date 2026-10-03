package k8shelpers

import (
	"context"
	"fmt"
	"io"
	"sync"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// TailLogs streams container logs to the supplied sink until the pod
// terminates. kind selects which container(s) to tail.
//
// For LogKindAll, tails every container concurrently and multiplexes
// with a [init:name] / [main] / [sidecar:name] prefix per line.
func TailLogs(ctx context.Context, k8s kubernetes.Interface, handle JobHandle, kind LogKind, sink io.Writer) error {
	pod, err := getPod(ctx, k8s, handle)
	if err != nil {
		return err
	}

	switch kind {
	case LogKindMain:
		if len(pod.Spec.Containers) == 0 {
			return fmt.Errorf("k8shelpers: no main containers in pod %s", pod.Name)
		}
		return streamLogs(ctx, k8s, handle.Namespace, pod.Name, pod.Spec.Containers[0].Name, "[main] ", sink)

	case LogKindMainRaw:
		if len(pod.Spec.Containers) == 0 {
			return fmt.Errorf("k8shelpers: no main containers in pod %s", pod.Name)
		}
		return streamLogs(ctx, k8s, handle.Namespace, pod.Name, pod.Spec.Containers[0].Name, "", sink)

	case LogKindInit:
		if len(pod.Spec.InitContainers) == 0 {
			return nil
		}
		var last error
		for _, ic := range pod.Spec.InitContainers {
			if err := streamLogs(ctx, k8s, handle.Namespace, pod.Name, ic.Name,
				fmt.Sprintf("[init:%s] ", ic.Name), sink); err != nil {
				last = err
			}
		}
		return last

	case LogKindAll:
		return tailAllContainers(ctx, k8s, pod, sink)

	default:
		if len(pod.Spec.Containers) == 0 {
			return fmt.Errorf("k8shelpers: no containers in pod %s", pod.Name)
		}
		return streamLogs(ctx, k8s, handle.Namespace, pod.Name, pod.Spec.Containers[0].Name, "", sink)
	}
}

func getPod(ctx context.Context, k8s kubernetes.Interface, handle JobHandle) (*corev1.Pod, error) {
	if handle.PodName != "" {
		return k8s.CoreV1().Pods(handle.Namespace).Get(ctx, handle.PodName, metav1.GetOptions{})
	}
	// Find pod by job label.
	list, err := k8s.CoreV1().Pods(handle.Namespace).List(ctx, metav1.ListOptions{
		LabelSelector: fmt.Sprintf("job-name=%s", handle.JobName),
	})
	if err != nil {
		return nil, err
	}
	if len(list.Items) == 0 {
		return nil, ErrPodNotScheduled
	}
	return &list.Items[0], nil
}

func streamLogs(ctx context.Context, k8s kubernetes.Interface, ns, podName, container, prefix string, sink io.Writer) error {
	req := k8s.CoreV1().Pods(ns).GetLogs(podName, &corev1.PodLogOptions{
		Container: container,
		Follow:    true,
	})
	stream, err := req.Stream(ctx)
	if err != nil {
		return fmt.Errorf("k8shelpers: get logs %s/%s: %w", podName, container, err)
	}
	defer stream.Close()

	if prefix == "" {
		_, err = io.Copy(sink, stream)
		return err
	}
	return copyWithPrefix(sink, stream, prefix)
}

// copyWithPrefix copies from src to dst, prepending prefix to each line.
func copyWithPrefix(dst io.Writer, src io.Reader, prefix string) error {
	buf := make([]byte, 4096)
	lineStart := true
	for {
		n, err := src.Read(buf)
		if n > 0 {
			for _, b := range buf[:n] {
				if lineStart {
					if _, werr := fmt.Fprint(dst, prefix); werr != nil {
						return werr
					}
					lineStart = false
				}
				if _, werr := dst.Write([]byte{b}); werr != nil {
					return werr
				}
				if b == '\n' {
					lineStart = true
				}
			}
		}
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
	}
}

func tailAllContainers(ctx context.Context, k8s kubernetes.Interface, pod *corev1.Pod, sink io.Writer) error {
	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		firstErr error
	)
	wrappedSink := &lockedWriter{mu: &mu, w: sink}

	record := func(err error) {
		if err != nil {
			mu.Lock()
			if firstErr == nil {
				firstErr = err
			}
			mu.Unlock()
		}
	}

	for _, ic := range pod.Spec.InitContainers {
		name := ic.Name
		wg.Add(1)
		go func() {
			defer wg.Done()
			record(streamLogs(ctx, k8s, pod.Namespace, pod.Name, name,
				fmt.Sprintf("[init:%s] ", name), wrappedSink))
		}()
	}
	for i, c := range pod.Spec.Containers {
		name := c.Name
		prefix := "[main] "
		if i > 0 {
			prefix = fmt.Sprintf("[sidecar:%s] ", name)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			record(streamLogs(ctx, k8s, pod.Namespace, pod.Name, name, prefix, wrappedSink))
		}()
	}
	wg.Wait()
	return firstErr
}

type lockedWriter struct {
	mu *sync.Mutex
	w  io.Writer
}

func (lw *lockedWriter) Write(p []byte) (int, error) {
	lw.mu.Lock()
	defer lw.mu.Unlock()
	return lw.w.Write(p)
}

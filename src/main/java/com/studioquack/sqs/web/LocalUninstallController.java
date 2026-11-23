package com.studioquack.sqs.web;

import com.studioquack.sqs.maintenance.UninstallService;
import com.studioquack.sqs.maintenance.UninstallService.UninstallSummary;
import jakarta.servlet.http.HttpServletRequest;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;
import java.util.Optional;

/**
 * Localhost-only ops endpoint for the copy-paste uninstall script path (
 * {@code POST /sqs/v1/ops/local-uninstall}). The generated uninstall snippet (see
 * {@code DaemonOpsPanel}) curls this BEFORE it {@code pkill}s / {@code rm}s the daemon, so the
 * OS-store secrets this daemon owns are purged even when the engine is torn down by hand rather
 * than vithe.
 */
@Slf4j
@RestController
@RequestMapping("/sqs/v1/ops")
public class LocalUninstallController {

    private final UninstallService uninstallService;

    public LocalUninstallController(UninstallService uninstallService) {
        this.uninstallService = uninstallService;
    }

    @PostMapping("/local-uninstall")
    public ResponseEntity<?> localUninstall(@RequestBody(required = false) Map<String, Object> body,
                                            HttpServletRequest request) {
        Optional<String> rejected = LoopbackOpsGuard.rejectReason(request);
        if (rejected.isPresent()) {
            log.warn("ops.local-uninstall: rejected caller remoteAddr={} reason={}",
                    request.getRemoteAddr(), rejected.get());
            return ResponseEntity.status(HttpStatus.FORBIDDEN)
                    .body(Map.of("error", "local-uninstall: " + rejected.get()));
        }
        boolean removeDependencies = body != null && Boolean.TRUE.equals(body.get("removeDependencies"));
        UninstallSummary summary = uninstallService.purge(removeDependencies);
        return ResponseEntity.ok(Map.of(
                "purgedSecrets", summary.purgedSecrets(),
                "dependenciesRemoved", summary.dependenciesRemoved(),
                "reapedContainers", summary.reapedContainers(),
                "notes", summary.notes()));
    }
}

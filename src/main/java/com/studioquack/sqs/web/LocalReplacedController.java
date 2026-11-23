package com.studioquack.sqs.web;

import com.studioquack.sqs.channel.HostReplacedReporter;
import jakarta.servlet.http.HttpServletRequest;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;
import java.util.Optional;

/**
 * Host-local op another engine's installer calls once the operator typed REPLACE (
 * {@code POST /sqs/v1/ops/replaced}, optional body {@code {"replacedBy":"<env id>"}}). The
 * installer then stops it and purges its identity, and StudioQuack offers the operator a fresh
 * install script for another machine — or deletion — instead of leaving an orphaned engine.
 */
@Slf4j
@RestController
@RequestMapping("/sqs/v1/ops")
public class LocalReplacedController {

    private final ObjectProvider<HostReplacedReporter> reporter;

    public LocalReplacedController(ObjectProvider<HostReplacedReporter> reporter) {
        this.reporter = reporter;
    }

    @PostMapping("/replaced")
    public ResponseEntity<?> replaced(@RequestBody(required = false) Map<String, Object> body,
                                      HttpServletRequest request) {
        Optional<String> rejected = LoopbackOpsGuard.rejectReason(request);
        if (rejected.isPresent()) {
            log.warn("ops.replaced: rejected caller remoteAddr={} reason={}", request.getRemoteAddr(), rejected.get());
            return ResponseEntity.status(HttpStatus.FORBIDDEN).body(Map.of("error", "replaced: " + rejected.get()));
        }
        HostReplacedReporter channelReporter = reporter.getIfAvailable();
        if (channelReporter == null) {
            return ResponseEntity.status(HttpStatus.CONFLICT)
                    .body(Map.of("reported", false, "error", "this engine is not enrolled with StudioQuack"));
        }
        Object replacedBy = body == null ? null : body.get("replacedBy");
        boolean reported = channelReporter.reportReplaced(replacedBy instanceof String s ? s : null);
        if (reported) {
            return ResponseEntity.ok(Map.of("reported", true));
        }
        return ResponseEntity.status(HttpStatus.BAD_GATEWAY)
                .body(Map.of("reported", false, "error", "StudioQuack did not accept the report — see the daemon log"));
    }
}

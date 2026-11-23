package com.studioquack.sqs.web;

import com.studioquack.sqs.channel.EnrollmentStatus;
import com.studioquack.sqs.retention.DiskSpaceProbe;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.env.Environment;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Liveness for installers and supervisors: {@code GET /sqs/v1/health} is anonymous and returns
 * only {@code {"status":"UP"}}. Tier, profiles and disk posture live on
 * {@code GET /sqs/v1/ops/health} behind {@link LoopbackOpsGuard}.
 */
@RestController
@RequestMapping("/sqs/v1")
public class HealthController {

    private final Environment environment;
    private final DiskSpaceProbe diskProbe;

    @Value("${sqs.tier:base}")
    private String tier;

    @Value("${sqs.log-retention.disk.high-watermark-percent:90}")
    private int diskHighWatermarkPercent;

    /** Enrolment outcome for the installer's final report; absent in slim test contexts. */
    private EnrollmentStatus enrollmentStatus;

    public HealthController(Environment environment, DiskSpaceProbe diskProbe) {
        this.environment = environment;
        this.diskProbe = diskProbe;
    }

    @Autowired(required = false)
    void setEnrollmentStatus(EnrollmentStatus enrollmentStatus) {
        this.enrollmentStatus = enrollmentStatus;
    }

    @GetMapping("/health")
    public Map<String, Object> health() {
        return Map.of("status", "UP");
    }

    @GetMapping("/ops/health")
    public ResponseEntity<?> detail(HttpServletRequest request) {
        Optional<String> rejected = LoopbackOpsGuard.rejectReason(request);
        if (rejected.isPresent()) {
            return ResponseEntity.status(HttpStatus.FORBIDDEN)
                    .body(Map.of("error", "health: " + rejected.get()));
        }
        return ResponseEntity.ok(detailBody());
    }

    Map<String, Object> detailBody() {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("status", "UP");
        body.put("component", "sq-client-daemon");
        body.put("tier", tier);
        body.put("profiles", environment.getActiveProfiles());

        int usagePercent = diskProbe.usagePercent();
        boolean diskPressure = usagePercent >= 0 && usagePercent >= diskHighWatermarkPercent;
        Map<String, Object> disk = new LinkedHashMap<>();
        disk.put("path", diskProbe.path());
        disk.put("usagePercent", usagePercent);
        disk.put("highWatermarkPercent", diskHighWatermarkPercent);
        disk.put("pressure", diskPressure);
        body.put("disk", disk);
        body.put("diskPressure", diskPressure);
        if (enrollmentStatus != null) {
            body.put("enrolment", enrollmentStatus.snapshot());
        }
        return body;
    }
}

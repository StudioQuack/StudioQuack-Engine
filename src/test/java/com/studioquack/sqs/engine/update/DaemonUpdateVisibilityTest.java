package com.studioquack.sqs.engine.update;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Operator visibility of a launcher's rollback to the previous jar: the update used to vanish
 * without a trace instead of reading FAILED.
 */
class DaemonUpdateVisibilityTest {

    @Test
    @DisplayName("a rollback (attempted sha != running sha) reads as a FAILED update with a reason")
    void rollbackReadsFailed() {
        String message = DaemonUpdateService.attemptRollbackMessage("aaaa1111bbbb2222cccc", "dddd3333eeee4444ffff");

        assertThat(message).contains("did not come up").contains("previous build was restored");
    }

    @Test
    @DisplayName("a landed update (attempted sha == running sha) is not a failure")
    void landedUpdateIsNotAFailure() {
        assertThat(DaemonUpdateService.attemptRollbackMessage("aaaa1111bbbb", "AAAA1111BBBB")).isNull();
        assertThat(DaemonUpdateService.attemptRollbackMessage(null, "aaaa")).isNull();
        assertThat(DaemonUpdateService.attemptRollbackMessage("aaaa", null)).isNull();
    }
}

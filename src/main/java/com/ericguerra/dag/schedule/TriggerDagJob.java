package com.ericguerra.dag.schedule;

import com.ericguerra.dag.service.DagService;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.JobExecutionContext;
import org.springframework.scheduling.quartz.QuartzJobBean;

import java.time.LocalDate;
import java.time.ZoneOffset;

/**
 * Fired by Quartz. The logical date is the day *before* the scheduled fire time (a 02:00 nightly run
 * processes yesterday's data), and is computed from the scheduled time rather than "now" so a misfire
 * that runs late still loads the right day.
 */
@DisallowConcurrentExecution
public class TriggerDagJob extends QuartzJobBean {
    private DagService dagService;

    public void setDagService(DagService dagService) { this.dagService = dagService; }

    @Override
    protected void executeInternal(JobExecutionContext ctx) {
        String dagId = ctx.getMergedJobDataMap().getString("dagId");
        LocalDate ds = ctx.getScheduledFireTime().toInstant().atZone(ZoneOffset.UTC).toLocalDate().minusDays(1);
        dagService.trigger(dagId, ds, "scheduler");
    }
}

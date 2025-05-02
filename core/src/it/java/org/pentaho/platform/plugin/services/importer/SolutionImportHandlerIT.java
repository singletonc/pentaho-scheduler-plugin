/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/

package org.pentaho.platform.plugin.services.importer;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.pentaho.metadata.repository.DomainAlreadyExistsException;
import org.pentaho.metadata.repository.DomainIdNullException;
import org.pentaho.metadata.repository.DomainStorageException;
import org.pentaho.platform.api.engine.IAuthorizationPolicy;
import org.pentaho.platform.api.engine.IUserRoleListService;
import org.pentaho.platform.api.scheduler2.IJob;
import org.pentaho.platform.api.scheduler2.IScheduler;
import org.pentaho.platform.api.scheduler2.SchedulerException;
import org.pentaho.platform.engine.core.system.PentahoSystem;
import org.pentaho.platform.engine.core.system.boot.PlatformInitializationException;
import org.pentaho.platform.engine.security.SecurityHelper;
import org.pentaho.platform.scheduler2.quartz.QuartzScheduler;
import org.pentaho.platform.scheduler2.quartz.test.StubUserRoleListService;
import org.pentaho.test.platform.engine.core.MicroPlatform;

import java.io.IOException;
import java.net.URL;
import java.util.Collections;
import java.util.List;

import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class SolutionImportHandlerIT extends Assert {

  private IScheduler scheduler;
  static final String TEST_USER = "TestUser";

  @Before
  public void init() throws PlatformInitializationException, SchedulerException {

    MicroPlatform mp = new MicroPlatform();

    mp.define( "IScheduler", TestQuartzScheduler.class );
    mp.define( IUserRoleListService.class, StubUserRoleListService.class );

    IAuthorizationPolicy policy = mock( IAuthorizationPolicy.class );
    when( policy.isAllowed( nullable( String.class ) ) ).thenReturn( true );
    mp.defineInstance( IAuthorizationPolicy.class, policy );

    mp.start();

    scheduler = PentahoSystem.get( IScheduler.class );
    scheduler.start();
  }

  @Test
  public void testImportSchedules()
    throws PlatformImportException, SchedulerException, DomainStorageException, DomainIdNullException,
    DomainAlreadyExistsException, IOException {
    SolutionImportHandler importHandler = spy( new SolutionImportHandler( Collections.emptyList() ) );

    importHandler.addImportHelper( new ScheduleImportUtil() );

    // Backup contains two schedules, one in Normal state, and another in Paused state.
    URL resourceUrl = getClass().getResource("schedules-backup.zip" );
    assert resourceUrl != null;
    String resourcePath = resourceUrl.getPath();

    RepositoryFileImportBundle importBundle = new RepositoryFileImportBundle.Builder()
      .path( resourcePath )
      .build();

    importHandler.importFile( importBundle );

    List<IJob> jobs = scheduler.getJobs( job -> true );

    assertEquals( 2, jobs.size() );
  }

  public static class TestQuartzScheduler extends QuartzScheduler {
    @Override protected String getCurrentUser() {
      SecurityHelper.getInstance().becomeUser( TEST_USER ); return super.getCurrentUser();
    }
  }
}

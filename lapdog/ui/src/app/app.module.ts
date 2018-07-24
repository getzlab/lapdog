import { BrowserModule } from '@angular/platform-browser';
import { NgModule } from '@angular/core';
import { RouterModule } from '@angular/router';
import { AppComponent } from './app.component';
import { RouterConfig } from './app.routes';
import { HttpModule } from '@angular/http';


import { HomeComponent } from './pages/home/home';

import { LapdogService } from './services/lapdog';

@NgModule({
  declarations: [
    AppComponent,
    HomeComponent
  ],
  imports: [
    BrowserModule,
    HttpModule,
    RouterModule.forRoot(RouterConfig),
  ],
  providers: [
    LapdogService
  ],
  bootstrap: [AppComponent]
})
export class AppModule { }

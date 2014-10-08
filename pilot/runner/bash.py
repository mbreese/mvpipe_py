import os
class BashRunner(object):
    def __init__(self, ):
        pass

    def run(self, jobs):
        if os.path.exists('/bin/bash'):
            print '#!/bin/bash'
        elif os.path.exists('/usr/bin/bash'):
            print '#!/usr/bin/bash'
        elif os.path.exists('/usr/local/bin/bash'):
            print '#!/usr/local/bin/bash'
        elif os.path.exists('/bin/sh'):
            print '#!/bin/sh'

        for i,j in enumerate(jobs):
            src = j.src(supress_pre=(i>0), supress_post=((i+2)<len(jobs)))
            if src:
                print src
